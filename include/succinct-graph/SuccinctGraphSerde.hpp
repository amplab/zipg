#ifndef SUCCINCT_GRAPH_SERDE_H
#define SUCCINCT_GRAPH_SERDE_H

#include <cstdint>
#include <string>

class SuccinctGraphSerde {
public:
    static std::string pad_int32(int32_t x);
    static std::string pad_int64(int64_t x);
};

#endif
