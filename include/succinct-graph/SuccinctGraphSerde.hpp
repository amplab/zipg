#ifndef SUCCINCT_GRAPH_SERDE_H
#define SUCCINCT_GRAPH_SERDE_H

#include <cstdint>
#include <map>
#include <string>

class SuccinctGraphSerde {
public:
    // Left-pads with zeros to 10 chars (with alphabet 0-9).
    static std::string pad_int32(int32_t x);

    // Left-pads with zeros to 20 chars (with alphabet 0-9).
    static std::string pad_int64(int64_t x);

    // Encodes decimal input to base alphabet, optionally padding the result
    // according to max_result_width (with leading 0).
    static std::string encode_int64(int64_t x, int max_result_width);

    // Decodes from base alphabet to decimal.
    static int64_t decode_int64(std::string& encoded);

    const static int WIDTH_INT32_PADDED = 10;
    const static int WIDTH_INT64_PADDED = 20;

private:
    const static std::string INT64_ENCODE_ALPHABET;
    const static int SIZE_INT64_ENCODE_ALPHABET;

    static std::map<char, int> init_map();
    static std::map<char, int> alphabet_char2pos;
};

#endif
