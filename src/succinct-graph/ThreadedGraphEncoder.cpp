#include "succinct-graph/SuccinctGraph.hpp"
#include "succinct-graph/ThreadedGraphEncoder.h"
#include "succinct-graph/utils.h"


void ThreadedGraphEncoder::construct_edge_file(
    const std::string edge_file,
    std::promise<void> promise,
    int saSamplingRate,
    int isaSamplingRate,
    int npaSamplingRate)
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

        graph.construct_edge_table(edge_file);

        // critical again
        lock.lock();
        --currActiveThreads_;
        LOG_E("Worker finished! Currently active #: %d\n", currActiveThreads_);
        lock.unlock();

        promise.set_value();
        hasFree_.notify_one();
        LOG_E("Notified!\n");
        return;
    } catch (...) {
        LOG_E("Worker encountered exception, about to exit(1)\n");
        promise.set_exception(std::current_exception());
        std::exit(1);
    }
}

int main(int argc, char **argv) {
    int maxConcurrentThreads = std::stoi(argv[1]);
    int saSamplingRate = std::stoi(argv[2]);
    int isaSamplingRate = std::stoi(argv[3]);
    int npaSamplingRate = std::stoi(argv[4]);
    LOG_E("Launching at most %d threads concurrently\n", maxConcurrentThreads);
    LOG_E("SA %d, ISA %d, NPA %d\n",
        saSamplingRate, isaSamplingRate, npaSamplingRate);

    ThreadedGraphEncoder encoder(maxConcurrentThreads);

    std::vector<std::promise<void>> promises;
    std::vector<std::future<void>> futures;
    std::vector<int> future_indices;

    std::map<int, bool> finished; // all false
    std::map<int, bool> to_be_submits; // init to all true
    for (int i = 5; i < argc; ++i) {
        to_be_submits[i - 5] = true;
    }

    int badAllocSleep = 60;
    int badAllocCount = 0;

    while (true) {
        // Check if every file is constructed
        bool all_finished = true;
        for (int i = 5; i < argc; ++i) {
            all_finished &= finished[i - 5];
        }
        if (all_finished) {
            LOG_E("All jobs done!\n");
            break;
        }

        // Find any job that can be submitted, and submit
        for (int i = 5; i < argc; ++i) {
            if (to_be_submits[i - 5]) {
                to_be_submits[i - 5] = false;
                std::string s(argv[i]);

                promises.emplace_back();
                futures.emplace_back(promises.back().get_future());
                future_indices.push_back(i - 5);

                // launch
                std::thread(&ThreadedGraphEncoder::construct_edge_file,
                    &encoder, s, std::move(promises.back()), 
                    saSamplingRate, isaSamplingRate, npaSamplingRate).detach();
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
