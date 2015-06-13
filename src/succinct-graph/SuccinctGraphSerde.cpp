#include "succinct-graph/SuccinctGraphSerde.hpp"

// Pads to 10 chars (with alphabet 0-9).
std::string SuccinctGraphSerde::pad_int32(int32_t x) {
    char res[11];
    sprintf(res, "%010d", x);
    return std::string(res);
}

// Pads to 20 chars (with alphabet 0-9).
std::string SuccinctGraphSerde::pad_int64(int64_t x) {
    char res[21];
    sprintf(res, "%020lld", x);
    return std::string(res);
}
