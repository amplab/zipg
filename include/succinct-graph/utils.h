#ifndef SUCCINCT_GRAPH_UTILS_H
#define SUCCINCT_GRAPH_UTILS_H

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

#endif
