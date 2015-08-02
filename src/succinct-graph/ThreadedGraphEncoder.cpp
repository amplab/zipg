#include "succinct-graph/SuccinctGraph.hpp"
#include "succinct-graph/ThreadedGraphEncoder.h"
#include "succinct-graph/utils.h"

void ThreadedGraphEncoder::construct_edge_file(
    const std::string& edge_file,
    int saSamplingRate,
    int isaSamplingRate,
    int npaSamplingRate)
{
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
    LOG_E("Waiting for this worker to finish\n");
    lock.unlock();

    graph.construct_edge_table(edge_file);

    // critical again
    lock.lock();
    --currActiveThreads_;
    LOG_E("Worker finished! Currently active #: %d\n", currActiveThreads_);
    lock.unlock();
    hasFree_.notify_all();
    LOG_E("notified!\n");

    return;
}

using std::shared_ptr;

int main(int argc, char **argv) {
    int maxConcurrentThreads = std::stoi(argv[1]);
    int saSamplingRate = std::stoi(argv[2]);
    int isaSamplingRate = std::stoi(argv[3]);
    int npaSamplingRate = std::stoi(argv[4]);
    LOG_E("Launching at most %d threads concurrently\n", maxConcurrentThreads);
    LOG_E("SA %d, ISA %d, NPA %d\n",
        saSamplingRate, isaSamplingRate, npaSamplingRate);

    ThreadedGraphEncoder encoder(maxConcurrentThreads);
    std::vector<shared_ptr<std::thread>> threads;
    std::map<int, bool> finished;

    int badAllocSleep = 60;
    int badAllocCount = 0;
    while (true) {
        bool allFinished = true;
        for (int i = 5; i < argc; ++i) {
            allFinished = allFinished && finished[i - 5];
        }
        if (allFinished) {
            LOG_E("All jobs done!\n");
            break;
        }

        try {
            for (int i = 5; i < argc; ++i) {
                if (!finished[i - 5]) {
                    std::string s(argv[i]);
                    threads.push_back(shared_ptr<std::thread>(new std::thread(
                        &ThreadedGraphEncoder::construct_edge_file,
                        &encoder, s,
                        saSamplingRate, isaSamplingRate, npaSamplingRate)));
                }
            }

            for (int i = 0; i < threads.size(); ++i) {
                if (threads[i]->joinable()) {
                    threads[i]->join();
                    finished[i] = true;
                }
            }
        } catch (const std::bad_alloc&) {
            ++badAllocCount;
            if (badAllocCount == 3) {
                LOG_E("bad_alloc 3 times, exiting\n");
                std::terminate();
            }
            badAllocSleep = 60 * badAllocCount;
            LOG_E("bad_alloc thrown (%d times), sleeping %d secs\n",
                badAllocCount, badAllocSleep);
            std::this_thread::sleep_for(std::chrono::seconds(badAllocSleep));
            continue;
        }
    }

    return 0;
}
