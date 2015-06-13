#include "succinct-graph/SuccinctGraphSerde.hpp"

#include <cassert>

const int SuccinctGraphSerde::SIZE_INT64_ENCODE_ALPHABET = 64; // 10+26+26+2
const std::string SuccinctGraphSerde::INT64_ENCODE_ALPHABET =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz\x03\x04";

std::map<char, int> SuccinctGraphSerde::alphabet_char2pos =
    SuccinctGraphSerde::init_map();

std::map<char, int> SuccinctGraphSerde::init_map() {
    alphabet_char2pos.clear();
    for (int i = 0; i < INT64_ENCODE_ALPHABET.length(); ++i) {
        alphabet_char2pos[INT64_ENCODE_ALPHABET.at(i)] = i;
    }
    assert(alphabet_char2pos.size() == SIZE_INT64_ENCODE_ALPHABET);
}

std::string SuccinctGraphSerde::pad_int32(int32_t x) {
    char res[11];
    sprintf(res, "%010d", x);
    return std::string(res);
}


std::string SuccinctGraphSerde::pad_int64(int64_t x) {
    char res[21];
    sprintf(res, "%020lld", x);
    return std::string(res);
}

std::string SuccinctGraphSerde::encode_int64(int64_t x, int max_result_width) {
    assert(INT64_ENCODE_ALPHABET.length() == SIZE_INT64_ENCODE_ALPHABET);
    std::string res((size_t) max_result_width, '0');
    int i = max_result_width - 1;
    while (x != 0) {
        res[i] = INT64_ENCODE_ALPHABET[x % SIZE_INT64_ENCODE_ALPHABET];
        x /= SIZE_INT64_ENCODE_ALPHABET;
        --i;
    }
    assert(i >= 0);
    return res;
}

int64_t SuccinctGraphSerde::decode_int64(std::string& encoded) {
    int64_t res = 0;
    int len = encoded.size();
    for (int i = 0; i < len; ++i) {
        res = res * SIZE_INT64_ENCODE_ALPHABET +
            alphabet_char2pos.at(encoded[i]);
    }
    return res;
}
