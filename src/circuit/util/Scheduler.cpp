/*
 * Scheduler.cpp
 *
 *  Created on: Aug 28, 2014
 *      Author: rlcevg
 */

#include "util/Scheduler.h"
#include "util/Utils.h"

namespace circuit {

#define MAX_JOB_THREADS		4
// circuitsync: a parallel or path job submitted in engine frame F is applied
// in frame F + JOB_DUE_FRAMES, never earlier or later (see NewJob).
#define JOB_DUE_FRAMES		1

CMultiQueue<CScheduler::WorkTask> CScheduler::workTasks;
CMultiQueue<CScheduler::PathTask> CScheduler::pathTasks;
// One lock for every instance's jobs: the worker and pathers are shared by all
// CircuitAI instances in the process, and a done job is waited on by its own.
spring::mutex CScheduler::jobMutex;
spring::condition_variable_any CScheduler::jobCond;
int CScheduler::engineFrame = -1;
spring::thread CScheduler::workerThread;
std::vector<spring::thread> CScheduler::patherThreads;
int CScheduler::maxPathThreads = 1;
std::atomic<bool> CScheduler::workerRunning(false);
unsigned int CScheduler::counterInstance = 0;

CScheduler::CScheduler()
		: lastFrame(-1)
		, isProcessing(false)
		, isWorkProcess(false)
		, numPathProcess(0)
{
	if (counterInstance == 0) {
		int maxThreads = spring::thread::hardware_concurrency();
		maxPathThreads = utils::clamp(maxThreads - 2, 1, MAX_JOB_THREADS);
	}
	counterInstance++;
	isRunning = true;
}

CScheduler::~CScheduler()
{
	Release();
}

void CScheduler::ProcessInit()
{
	for (auto& task : initTasks) {
		task->Run();
	}
	// initTasks.clear();
}

void CScheduler::ProcessRelease()
{
	for (auto& task : releaseTasks) {
		task->Run();
	}

	Release();
}

void CScheduler::Release()
{
	if (!isRunning) {
		return;
	}
	isRunning = false;
	counterInstance--;

	std::weak_ptr<CScheduler>& scheduler = self;
	workTasks.RemoveAllIf([&scheduler](WorkTask& item) -> bool {
		return !scheduler.owner_before(item.scheduler) && !item.scheduler.owner_before(scheduler);
	});
	pathTasks.RemoveAllIf([&scheduler](PathTask& item) -> bool {
		return !scheduler.owner_before(item.scheduler) && !item.scheduler.owner_before(scheduler);
	});

	if (counterInstance == 0 && workerRunning.load()) {
		workerRunning = false;
		// At this point workTasks is empty. Push empty task in case worker stuck at Pop().
		workTasks.Push({self, nullptr});
		if (workerThread.joinable()) {
			workerThread.join();
		}
		workTasks.Clear();

		for (unsigned int i = 0; i < patherThreads.size(); ++i) {
			pathTasks.Push({self, nullptr});
		}
		for (std::thread& t : patherThreads) {
			if (t.joinable()) {
				t.join();
			}
		}
		patherThreads.clear();
		pathTasks.Clear();
	}

	barrier.Wait([this]() { return !isWorkProcess && (numPathProcess == 0); });

	jobs.clear();  // circuitsync: released, never applied
}

void CScheduler::StartThreads()
{
	if (workerRunning.load()) {
		return;
	}
	workerRunning = true;

	workerThread = spring::thread(&CScheduler::WorkerThread);

	assert(patherThreads.empty());
	for (int i = 0; i < maxPathThreads; ++i) {
		std::thread t = spring::thread(&CScheduler::PatherThread, i);
		patherThreads.push_back(std::move(t));
	}
}

void CScheduler::RunTaskEvery(const std::shared_ptr<CGameTask>& task, int frameInterval, int frameOffset)
{
	if (frameOffset > 0) {
		RunTaskAfter(std::make_shared<CGameTask>([this, task, frameInterval]() {
			repeatTasks.push_back({task, frameInterval, lastFrame});
		}), frameOffset);
	} else {
		repeatTasks.push_back({task, frameInterval, lastFrame});
	}
}

void CScheduler::ProcessTasks(int frame)
{
	isProcessing = true;
	lastFrame = frame;

	// Process once tasks
	std::list<OnceTask>::iterator ionce = onceTasks.begin();
	while (ionce != onceTasks.end()) {
		if (ionce->frame <= frame) {
			ionce->task->Run();
			ionce = onceTasks.erase(ionce);  // alternatively, onceTasks.erase(iter++);
		} else {
			++ionce;
		}
	}

	// Process repeat tasks
	for (auto& container : repeatTasks) {
		if (frame - container.lastFrame >= container.frameInterval) {
			container.task->Run();
			container.lastFrame = frame;
		}
	}

	// circuitsync: apply every parallel and path job due by this frame, in
	// submission order, waiting for any not done yet. Stock applied at most one
	// parallel result per frame, path results only on frames without one, and
	// each whenever its thread happened to finish.
	ApplyDueJobs(frame);

	// Update task queues
	if (!removeTasks.empty()) {
		for (auto& task : removeTasks) {
			onceTasks.remove({task, 0});
			repeatTasks.remove({task, 0, 0});
		}
		removeTasks.clear();
	}

	isProcessing = false;
}

std::shared_ptr<CScheduler::SJob> CScheduler::NewJob()
{
	std::shared_ptr<SJob> job = std::make_shared<SJob>();
	// One frame: the job overlaps the rest of this frame (other AIs, our brain,
	// the sim of the next frame) and the main thread blocks only if it is
	// still running when this AI's next Update comes. A job submitted before
	// ProcessTasks in an Update (AllyTeam's threat/influence/enemy refresh) is
	// tagged by the engine frame, not by lastFrame, so it too gets a frame.
	job->dueFrame = engineFrame + JOB_DUE_FRAMES;
	jobs.push_back(job);
	return job;
}

void CScheduler::ApplyDueJobs(int frame)
{
	// Due frames never decrease in submission order, so the due jobs are a prefix.
	size_t numDue = 0;
	while ((numDue < jobs.size()) && (jobs[numDue]->dueFrame <= frame)) {
		++numDue;
	}
	if (numDue == 0) {
		return;
	}

	// Every due job is done before any is applied: an apply may change what a
	// job still running reads (a buffer swap, say).
	{
		std::unique_lock<spring::mutex> lock(jobMutex);
		size_t i = 0;
		jobCond.wait(lock, [this, numDue, &i]() {
			while ((i < numDue) && jobs[i]->isDone) {
				++i;
			}
			return i == numDue;
		});
	}

	// Off the queue before applying: an apply may submit jobs (due next frame).
	std::vector<std::shared_ptr<SJob>> due(jobs.begin(), jobs.begin() + numDue);
	jobs.erase(jobs.begin(), jobs.begin() + numDue);
	for (std::shared_ptr<SJob>& job : due) {
		if (job->query != nullptr) {
			// Stock skipped a query nobody holds any more; ours is the job's reference.
			if ((job->onPathed != nullptr) && (job->query.use_count() > 1)) {
				job->onPathed(job->query);
			}
		} else if (job->onComplete != nullptr) {
			job->onComplete->Run();
		}
		job = nullptr;  // destroyed here, on the main thread
	}
}

void CScheduler::FinishJob(std::shared_ptr<SJob>&& job)
{
	{
		// The thread's reference goes under the lock, before isDone: the main
		// thread sees isDone only after it, so the main thread's reference is
		// the last and the job (closures, query) is destroyed there.
		std::lock_guard<spring::mutex> lock(jobMutex);
		job->isDone = true;
		job = nullptr;
	}
	jobCond.notify_all();
}

void CScheduler::RunParallelTask(const std::shared_ptr<CGameTask>& task, const std::shared_ptr<CGameTask>& onComplete)
{
	StartThreads();
	std::shared_ptr<SJob> job = NewJob();
	job->task = task;
	job->onComplete = onComplete;
	workTasks.Push({self, job});
}

void CScheduler::RunPathTask(const std::shared_ptr<IPathQuery>& query, PathFunc&& task, PathedFunc&& onComplete)
{
	StartThreads();
	std::shared_ptr<SJob> job = NewJob();
	job->query = query;
	job->pathTask = std::move(task);
	job->onPathed = std::move(onComplete);
	pathTasks.Push({self, job});
}

void CScheduler::RemoveTask(const std::shared_ptr<CGameTask>& task)
{
	if (isProcessing) {
		removeTasks.push_back(task);
	} else {
		onceTasks.remove({task, 0});
		repeatTasks.remove({task, 0, 0});
	}
}

void CScheduler::WorkerThread()
{
	while (workerRunning.load()) {
		WorkTask container = workTasks.Pop();
		if (container.job == nullptr) {
			continue;  // shutdown wake-up
		}

		std::shared_ptr<CScheduler> scheduler = container.scheduler.lock();
		if (scheduler != nullptr) {
			scheduler->barrier.NotifyOne([scheduler]() { scheduler->isWorkProcess = true; });
			if (scheduler->isRunning) {

				container.job->task->Run();

			}
			FinishJob(std::move(container.job));
			scheduler->barrier.NotifyOne([scheduler]() { scheduler->isWorkProcess = false; });
		} else {
			FinishJob(std::move(container.job));
		}
	}
}

void CScheduler::PatherThread(int num)
{
	while (workerRunning.load()) {
		PathTask container = pathTasks.Pop();
		if (container.job == nullptr) {
			continue;  // shutdown wake-up
		}

		std::shared_ptr<CScheduler> scheduler = container.scheduler.lock();
		if (scheduler == nullptr) {
			FinishJob(std::move(container.job));
			continue;
		}

		scheduler->barrier.NotifyOne([scheduler]() { scheduler->numPathProcess++; });
		// A query only the job holds is dropped at apply anyway (ApplyDueJobs);
		// skipping it here saves the work and changes nothing.
		if (scheduler->isRunning && (container.job->query.use_count() > 1)) {

			container.job->pathTask(container.job->query, num);

		}
		FinishJob(std::move(container.job));
		scheduler->barrier.NotifyOne([scheduler]() { scheduler->numPathProcess--; });
	}
}

} // namespace circuit
