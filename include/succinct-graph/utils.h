#ifndef SUCCINCT_GRAPH_UTILS_H
#define SUCCINCT_GRAPH_UTILS_H

#include <sys/time.h>

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
    return  now.tv_usec + (time_t)now.tv_sec * 1000000;
}

#endif
