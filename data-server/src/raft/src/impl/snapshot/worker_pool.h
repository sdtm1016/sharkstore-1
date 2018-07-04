_Pragma("once");

#include <memory>
#include <vector>
#include <list>
#include <mutex>

namespace sharkstore {
namespace raft {
namespace impl {

class SnapWorker;
class SnapTask;

class SnapWorkerPool final {
public:
    SnapWorkerPool(const std::string& name, size_t size);
    ~SnapWorkerPool();

    SnapWorkerPool(const SnapWorkerPool&) = delete;
    SnapWorkerPool& operator=(const SnapWorkerPool&) = delete;

    bool Post(const std::shared_ptr<SnapTask>& task);
    size_t Runnings() const;

private:
    friend class SnapWorker;

    void addToFreeList(SnapWorker* w);

private:
    std::vector<SnapWorker*> workers_;

    std::list<SnapWorker*> free_list_;
    mutable std::mutex mu_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
