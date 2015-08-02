#include "succinct-graph/SuccinctGraph.hpp"
#include "succinct-graph/ThreadedGraphEncoder.hpp"
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
    SuccinctGraph graph(""); // no-op
    graph.set_sa_sampling_rate(saSamplingRate);
    graph.set_isa_sampling_rate(isaSamplingRate);
    graph.set_npa_sampling_rate(npaSamplingRate);
    std::thread t(&SuccinctGraph::construct_edge_table, graph, edge_file);
    LOG_E("Waiting for this worker to finish\n");
    lock.unlock();

    // non-critical
    t.join();

    // critical again
    lock.lock();
    --currActiveThreads_;
    lock.unlock();
    LOG_E("Worker finished! Currently active #: %d\n", currActiveThreads_);
    hasFree_.notify_all();

    return;
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
    for (int i = 2; i < argc; ++i) {
        std::string s = argv[i];
        LOG_E("Submitting edge file '%s' to be encoded\n", s.c_str());
        encoder.construct_edge_file(
            s, saSamplingRate, isaSamplingRate, npaSamplingRate);
    }

    return 0;
}
