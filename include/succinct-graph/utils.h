#ifndef SUCCINCT_GRAPH_UTILS_H
#define SUCCINCT_GRAPH_UTILS_H

#ifndef LOG_E
#define LOG_E(fmt, ...) fprintf(stderr, fmt, ##__VA_ARGS__)
#endif

void print_vector(const std::string& msg, const std::vector<int64_t>& vec) {
    printf("%s[", msg.c_str());
    for (auto it = vec.begin(); it != vec.end(); ++it)
        printf(" %lld", *it);
    printf(" ]\n");
}

#endif
