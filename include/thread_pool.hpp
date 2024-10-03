#ifndef THREAD_POOL_HPP
#define THREAD_POOL_HPP

#include <thread>
#include <future>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <vector>

class ThreadPool {
public:
    ThreadPool(size_t max_threads);
    ~ThreadPool();

    // Overload for non-void return types
    template <typename F>
    auto enqueue(F&& f) -> std::future<typename std::invoke_result<F>::type>;

    // Overload for void return types
    template <typename F>
    void enqueue_void(F&& f);

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};

// Template function definitions must stay in the header
template <typename F>
auto ThreadPool::enqueue(F&& f) -> std::future<typename std::invoke_result<F>::type> {
    using return_type = typename std::invoke_result<F>::type;

    auto task = std::make_shared<std::packaged_task<return_type()>>(std::forward<F>(f));
    std::future<return_type> res = task->get_future();

    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        tasks.emplace([task]() { (*task)(); });
    }

    condition.notify_one();
    return res;
}

template <typename F>
void ThreadPool::enqueue_void(F&& f) {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        tasks.emplace(std::forward<F>(f));
    }
    condition.notify_one();
}

#endif // THREAD_POOL_HPP
