_Pragma("once");

#include <memory>
#include <vector>
#include <list>
#include <map>
#include <mutex>
#include "task.h"

namespace sharkstore {
namespace raft {
namespace impl {

class SnapWorker;

class SnapWorkerPool final {
public:
    SnapWorkerPool(const std::string& name, size_t size);
    ~SnapWorkerPool();

    SnapWorkerPool(const SnapWorkerPool&) = delete;
    SnapWorkerPool& operator=(const SnapWorkerPool&) = delete;

    bool Post(const SnapTaskPtr& task);

    size_t RunningsCount() const;

    void GetRunningTasks(std::vector<SnapTaskPtr> *tasks);

private:
    class FreeList {
    public:
        void Add(SnapWorker *w);
        SnapWorker* Get();

    private:
        std::list<SnapWorker*> workers_;
        std::mutex mu_;
    };

    class RunningMap {
    public:
        size_t Size() const;
        void Add(SnapTaskPtr task);
        void Remove(const SnapTaskPtr& task);
        void GetAll(std::vector<SnapTaskPtr> *tasks);

    private:
        std::map<std::string, SnapTaskPtr> tasks_;
        mutable std::mutex mu_;
    };

private:
    friend class SnapWorker;

    void addFreeWorker(SnapWorker* w);

    void addRunning(const SnapTaskPtr& task);
    void removeRunning(const SnapTaskPtr& task);

private:
    std::vector<SnapWorker*> all_workers_;

    FreeList free_workers_;
    RunningMap running_tasks_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
