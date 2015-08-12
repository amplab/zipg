#ifndef THREADED_GRAPH_ENCODER_H
#define THREADED_GRAPH_ENCODER_H

#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

#include "utils.h"

// Encode graph shards using Succinct concurrently.  Threading is needed to
// avoid excessive memory consumption.  As a simple rule of thumb, the peak
// memory usage during the encoding can be 10-20x of the original input.
class ThreadedGraphEncoder {
public:

    ThreadedGraphEncoder(int maxConcurrentThreads)
    : maxConcurrentThreads_(maxConcurrentThreads),
      currActiveThreads_(0),
      mutex_(),
      hasFree_()
    {
    }

    void construct_edge_file(
        const std::string edge_file,
        std::promise<void> promise,
        int saSamplingRate,
        int isaSamplingRate,
        int npaSamplingRate);

private:

    int maxConcurrentThreads_;
    int currActiveThreads_;

    // For maxConcurrentThreads_ and currActiveThreads_.
    std::mutex mutex_;
    // Is there any free thread to use?
    std::condition_variable hasFree_;

};

#endif
