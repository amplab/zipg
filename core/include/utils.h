#ifndef SUCCINCT_GRAPH_UTILS_H
#define SUCCINCT_GRAPH_UTILS_H

#include <sys/stat.h>
#include <sys/time.h>

// For proper (u)int64t printing
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#ifndef LOG_E
#define LOG_E(fmt, ...) fprintf(stderr, fmt, ##__VA_ARGS__)
#endif

#ifndef COND_LOG_E
#ifdef LOG_DEBUG
#define COND_LOG_E(fmt, ...) fprintf(stderr, fmt, ##__VA_ARGS__)
#else
#define COND_LOG_E(fmt, ...) /* no-op */
#endif
#endif

inline time_t get_timestamp() {
    struct timeval now;
    gettimeofday (&now, NULL);
    return now.tv_usec + (time_t)now.tv_sec * 1000000;
}

class scoped_timer {
public:
    scoped_timer(int64_t* latency) {
        this->latency_ = latency;
        this->start_ = get_timestamp();
    }
    ~scoped_timer() {
        *(this->latency_) = (get_timestamp() - start_); // microsecs
    }
private:
    int64_t* latency_;
    time_t start_;
};

inline bool file_or_dir_exists(const std::string& pathname) {
    struct stat buffer;
    return (stat(pathname.c_str(), &buffer) == 0);
}

enum StoreMode {
    SuccinctStore,
    SuffixStore,
    LogStore
};

#endif
