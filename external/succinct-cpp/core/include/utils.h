#ifndef LOG_UTILS_H
#define LOG_UTILS_H

#include <sys/stat.h>
#include <sys/time.h>

#include <string>

// For proper (u)int64t printing
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#define LOG_DEBUG

#ifndef LOG_E
#define LOG_E(fmt, ...) fprintf(stderr, fmt, ##__VA_ARGS__); fflush(stderr)
#endif

#ifndef COND_LOG_E
#ifdef LOG_DEBUG
#define COND_LOG_E(fmt, ...) fprintf(stderr, fmt, ##__VA_ARGS__); fflush(stderr)
#else
#define COND_LOG_E(fmt, ...) /* no-op */
#endif
#endif

#endif
