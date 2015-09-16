#include "SuccinctGraph.hpp"
#include "ThreadedGraphEncoder.h"
#include "utils.h"
#include "utils/thread_pool.h"

void ThreadedGraphEncoder::construct_edge_file(
    const std::string edge_file,
    std::promise<void> promise,
    int saSamplingRate,
    int isaSamplingRate,
    int npaSamplingRate,
    bool edge_table_only)
{
    try {
        std::unique_lock<std::mutex> lock(mutex_);
        hasFree_.wait(lock, [this] {
            return currActiveThreads_ < maxConcurrentThreads_;
        });

        // critical section: lk is acquired
        ++currActiveThreads_;
        LOG_E("Acquired thread! Currently active #: %d\n", currActiveThreads_);
        LOG_E("Submitting edge file '%s' to be encoded\n", edge_file.c_str());
        SuccinctGraph graph(""); // no-op
        graph.set_sa_sampling_rate(saSamplingRate);
        graph.set_isa_sampling_rate(isaSamplingRate);
        graph.set_npa_sampling_rate(npaSamplingRate);
        lock.unlock();

        graph.construct_edge_table(edge_file, edge_table_only);

        // critical again
        lock.lock();
        --currActiveThreads_;
        LOG_E("Worker finished! Currently active #: %d\n", currActiveThreads_);
        lock.unlock();

        promise.set_value();
        hasFree_.notify_one();
        LOG_E("Notified!\n");
    } catch (...) {
        LOG_E("Worker encountered exception, about to exit\n");
        promise.set_exception(std::current_exception());
    }
    return;
}

int main(int argc, char **argv) {
    int maxConcurrentThreads = std::stoi(argv[1]);
    int saSamplingRate = std::stoi(argv[2]);
    int isaSamplingRate = std::stoi(argv[3]);
    int npaSamplingRate = std::stoi(argv[4]);
    int encode_type = std::stoi(argv[5]);
    bool edge_table_only = (std::stoi(argv[6]) == 1) ? true : false;
    const int restArgsPtr = 7;

    LOG_E("Launching at most %d threads concurrently\n", maxConcurrentThreads);
    LOG_E("SA %d, ISA %d, NPA %d; encode type %d\n",
        saSamplingRate, isaSamplingRate, npaSamplingRate, encode_type);

    if (encode_type == 1) {
        // case: node table; use simple thread pool and don't catch exceptions
        ThreadPool pool(maxConcurrentThreads);
        for (int i = restArgsPtr; i < argc; ++i) {
            std::string node_file(argv[i]);
            pool.Enqueue([=] {
                SuccinctGraph graph(""); // no-op
                graph.set_sa_sampling_rate(saSamplingRate);
                graph.set_isa_sampling_rate(isaSamplingRate);
                graph.set_npa_sampling_rate(npaSamplingRate);
                graph.construct_node_table(node_file);
                LOG_E("Node file '%s' Succinct-constructed!\n",
                    node_file.c_str());
            });
        }
        pool.ShutDown();
        return 0;
    }

    ThreadedGraphEncoder encoder(maxConcurrentThreads);

    std::vector<std::promise<void>> promises;
    std::vector<std::future<void>> futures;
    std::vector<int> future_indices;

    std::map<int, bool> finished; // all false
    std::map<int, bool> to_be_submits; // init to all true
    for (int i = restArgsPtr; i < argc; ++i) {
        to_be_submits[i - restArgsPtr] = true;
    }

    int badAllocSleep = 60;
    int badAllocCount = 0;

    while (true) {
        // Check if every file is constructed
        bool all_finished = true;
        for (int i = restArgsPtr; i < argc; ++i) {
            all_finished &= finished[i - restArgsPtr];
        }
        if (all_finished) {
            LOG_E("All jobs done!\n");
            break;
        }

        // Find any job that can be submitted, and submit
        for (int i = restArgsPtr; i < argc; ++i) {
            if (to_be_submits[i - restArgsPtr]) {
                to_be_submits[i - restArgsPtr] = false;
                std::string s(argv[i]);

                promises.emplace_back();
                futures.emplace_back(promises.back().get_future());
                future_indices.push_back(i - restArgsPtr);

                // launch
                std::thread(&ThreadedGraphEncoder::construct_edge_file,
                    &encoder, s, std::move(promises.back()),
                    saSamplingRate, isaSamplingRate, npaSamplingRate,
                    edge_table_only).detach();
            }
        }

        // Wait for running jobs
        size_t i;
        try {
            for (i = 0; i < futures.size(); ++i) {
                if (futures[i].valid()) {
                    futures[i].get();

                    // if no exception is thrown, then the task succeeds
                    finished[future_indices[i]] = true;
                }
            }
        } catch (...) {
            to_be_submits[future_indices[i]] = true; // needs to be re-submitted

            ++badAllocCount;
            if (badAllocCount == 3) {
                LOG_E("exception (likely bad_alloc) 3 times, exiting\n");
                std::terminate();
            }
            badAllocSleep = 60 * badAllocCount;
            LOG_E("exception thrown (%d times), sleeping for %d secs\n",
                badAllocCount, badAllocSleep);
            std::this_thread::sleep_for(std::chrono::seconds(badAllocSleep));
            continue;
        }
    }

    return 0;
}
