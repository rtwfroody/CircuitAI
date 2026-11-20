/*
 * Scheduler.h
 *
 *  Created on: Aug 28, 2014
 *      Author: rlcevg
 */

#ifndef SRC_CIRCUIT_UTIL_SCHEDULER_H_
#define SRC_CIRCUIT_UTIL_SCHEDULER_H_

#include "util/MultiQueue.h"
#include "util/GameTask.h"
#include "util/Defines.h"

#include "System/Threading/SpringThreading.h"

#include <functional>
#include <memory>
#include <list>
#include <deque>

namespace circuit {

class IPathQuery;

class Barrier {
public:
	template<typename _Predicate>
	void NotifyOne(_Predicate __p) {
		{
			std::lock_guard<spring::mutex> mlock(_mutex);
			__p();
		}
		_cond.notify_one();
	}
	template<typename _Predicate>
	void Wait(_Predicate __p) {
		std::unique_lock<spring::mutex> mlock(_mutex);
		_cond.wait(mlock, __p);
	}

private:
	spring::mutex _mutex;
	spring::condition_variable_any _cond;
};

class CScheduler {
public:
	CScheduler();
	virtual ~CScheduler();

	void Init(const std::shared_ptr<CScheduler>& thisPtr) { self = thisPtr; }
	void ProcessInit();
	void ProcessRelease();

private:
	void Release();
	void StartThreads();

public:
	using PathFunc = std::function<void (const std::shared_ptr<IPathQuery>& query, int threadNum)>;
	using PathedFunc = std::function<void (const std::shared_ptr<IPathQuery>& query)>;

	/*
	 * Add task at specified frame, or execute immediately at next frame
	 */
	void RunTaskAt(const std::shared_ptr<CGameTask>& task, int frame = 0) {
		onceTasks.push_back({task, frame});
	}

	/*
	 * Add task at frame relative to current frame
	 */
	void RunTaskAfter(const std::shared_ptr<CGameTask>& task, int frame = 0) {
		onceTasks.push_back({task, lastFrame + frame});
	}

	/*
	 * Add task at specified interval
	 */
	void RunTaskEvery(const std::shared_ptr<CGameTask>& task, int frameInterval = FRAMES_PER_SEC, int frameOffset = 0);

	/*
	 * Process queued tasks at specified frame
	 */
	void ProcessTasks(int frame);

	/*
	 * Run concurrent task, finalize on success at main thread
	 */
	void RunParallelTask(const std::shared_ptr<CGameTask>& task, const std::shared_ptr<CGameTask>& onComplete = nullptr);

	/*
	 * Run concurrent pathfinder, finalize on complete at main thread
	 */
	void RunPathTask(const std::shared_ptr<IPathQuery>& query, PathFunc&& task, PathedFunc&& onComplete = nullptr);

	/*
	 * Remove scheduled task from queue
	 */
	void RemoveTask(const std::shared_ptr<CGameTask>& task);

	/*
	 * Run task on init. Not affected by RemoveTask
	 */
	void RunOnInit(std::shared_ptr<CGameTask>&& task) {
		initTasks.push_back(task);
	}

	/*
	 * Run task on release. Not affected by RemoveTask
	 */
	void RunOnRelease(std::shared_ptr<CGameTask>&& task) {
		releaseTasks.push_back(task);
	}

	int GetMaxPathThreads() const { return maxPathThreads; }

	/*
	 * circuitsync: the engine frame now being played, set at the start of every
	 * CCircuitAI::Update. A job submitted in frame F is due in frame F + JOB_DUE_FRAMES.
	 */
	static void SetEngineFrame(int frame) { engineFrame = frame; }

private:
	std::weak_ptr<CScheduler> self;
	int lastFrame;
	bool isProcessing;  // regular CGameTask

	bool isWorkProcess;  // parallel CGameTask
	int numPathProcess;  // parallel PathFunc
	Barrier barrier;
	std::atomic<bool> isRunning;  // parallel

	struct BaseContainer {
		BaseContainer(const std::shared_ptr<CGameTask>& task) : task(task) {}
		std::shared_ptr<CGameTask> task;
		bool operator==(const BaseContainer& other) const {
			return task == other.task;
		}
	};
	struct OnceTask: public BaseContainer {
		OnceTask(const std::shared_ptr<CGameTask>& task, int frame)
			: BaseContainer(task), frame(frame) {}
		int frame;
	};
	std::list<OnceTask> onceTasks;

	struct RepeatTask: public BaseContainer {
		RepeatTask(const std::shared_ptr<CGameTask>& task, int frameInterval, int lastFrame)
			: BaseContainer(task), frameInterval(frameInterval), lastFrame(lastFrame) {}
		int frameInterval;
		int lastFrame;
	};
	std::list<RepeatTask> repeatTasks;

	std::vector<std::shared_ptr<CGameTask>> removeTasks;

	/*
	 * circuitsync: one parallel or path job. The main thread keeps every job of
	 * this scheduler in `jobs`, in submission order, and applies a job's result
	 * only in its due frame, after it and every job due with it is done. So
	 * what Circuit does depends on sim frames alone, never on how fast the
	 * threads ran or which pather finished first.
	 *
	 * The job owns everything the main thread made for it (tasks, callbacks,
	 * the path query), so all of it is destroyed on the main thread at apply;
	 * the threads drop their reference before marking the job done.
	 */
	struct SJob {
		int dueFrame;
		bool isDone = false;  // guarded by jobMutex
		// parallel job
		std::shared_ptr<CGameTask> task;
		std::shared_ptr<CGameTask> onComplete;
		// path job: query is held here so that the main thread destroys it;
		// "anyone else still holds it" (use_count() > 1) is what the stock
		// weak_ptr test meant
		std::shared_ptr<IPathQuery> query;
		PathFunc pathTask;
		PathedFunc onPathed;
	};
	std::deque<std::shared_ptr<SJob>> jobs;  // main thread only

	std::shared_ptr<SJob> NewJob();
	void ApplyDueJobs(int frame);
	static void FinishJob(std::shared_ptr<SJob>&& job);

	struct WorkTask {
		WorkTask(const std::weak_ptr<CScheduler>& scheduler, const std::shared_ptr<SJob>& job)
			: scheduler(scheduler), job(job) {}
		std::weak_ptr<CScheduler> scheduler;
		std::shared_ptr<SJob> job;  // nullptr: wake-up at shutdown
	};
	static CMultiQueue<WorkTask> workTasks;

	struct PathTask {
		PathTask(const std::weak_ptr<CScheduler>& scheduler, const std::shared_ptr<SJob>& job)
			: scheduler(scheduler), job(job) {}
		std::weak_ptr<CScheduler> scheduler;
		std::shared_ptr<SJob> job;  // nullptr: wake-up at shutdown
	};
	static CMultiQueue<PathTask> pathTasks;

	static spring::mutex jobMutex;
	static spring::condition_variable_any jobCond;
	static int engineFrame;

	std::vector<std::shared_ptr<CGameTask>> initTasks;
	std::vector<std::shared_ptr<CGameTask>> releaseTasks;

	static spring::thread workerThread;
	static std::vector<spring::thread> patherThreads;
	static int maxPathThreads;
	static std::atomic<bool> workerRunning;
	static unsigned int counterInstance;

	static void WorkerThread();
	static void PatherThread(int num);
};

} // namespace circuit

#endif // SRC_CIRCUIT_UTIL_SCHEDULER_H_
