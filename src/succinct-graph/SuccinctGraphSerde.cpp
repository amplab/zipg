#include "succinct-graph/SuccinctGraphSerde.hpp"

#include <cassert>

const int SuccinctGraphSerde::SIZE_ENCODE_ALPHABET = 64; // 10+26+26+2
const std::string SuccinctGraphSerde::ENCODE_ALPHABET =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz\x03\x04";

std::string SuccinctGraphSerde::pad_node_id(int64_t x) {
    return pad_int64(x);
}

std::string SuccinctGraphSerde::pad_atype(int64_t x) {
    return pad_int64(x);
}

std::string SuccinctGraphSerde::pad_edge_width(int32_t x) {
    return pad_int32(x);
}

std::string SuccinctGraphSerde::pad_data_width(int64_t x) {
    return pad_int64(x);
}

// Uses exactly 2 chars to represent dst id width.
std::string SuccinctGraphSerde::pad_dst_id_width(int32_t x) {
    assert(x <= WIDTH_NODE_ID_PADDED);
    if (x < 10) return '0' + std::to_string(x);
    return std::to_string(x);
}

std::string SuccinctGraphSerde::encode_timestamp(int64_t timestamp) {
#if ALPHABET_ENCODE
    return encode_int64(timestamp, WIDTH_TIMESTAMP);
#else
    return pad_int64(timestamp);
#endif
}

int64_t SuccinctGraphSerde::decode_timestamp(const std::string& encoded) {
#if ALPHABET_ENCODE
    return decode_int64(encoded);
#else
    return std::stol(encoded);
#endif
}

std::vector<int64_t> SuccinctGraphSerde::decode_multi_timestamps(
        const std::string& encoded) {
#if ALPHABET_ENCODE
    return decode_multi_int64(encoded, WIDTH_TIMESTAMP);
#else
    return unpad_multi_int64(encoded);
#endif
}

std::string SuccinctGraphSerde::encode_node_id(int64_t node_id) {
#if ALPHABET_ENCODE
    return encode_int64(node_id, WIDTH_NODE_ID);
#else
    return pad_int64(node_id);
#endif
}

// TODO: code duplication.
std::string SuccinctGraphSerde::encode_node_id(
    int64_t node_id,
    int32_t padded_width) {

    char res[padded_width + 1];
    sprintf(res, "%0*lld", padded_width, node_id);
    return std::string(res);
}

int64_t SuccinctGraphSerde::decode_node_id(const std::string& encoded) {
#if ALPHABET_ENCODE
    return decode_int64(encoded);
#else
    return std::stol(encoded);
#endif
}

std::vector<int64_t> SuccinctGraphSerde::decode_multi_node_ids(
    const std::string& encoded) {
#if ALPHABET_ENCODE
    return decode_multi_int64(encoded, WIDTH_NODE_ID);
#else
    return unpad_multi_int64(encoded);
#endif
}

// TODO: code duplication.
std::vector<int64_t> SuccinctGraphSerde::decode_multi_node_ids(
    const std::string& encoded,
    int32_t padded_width) {

    assert(encoded.length() % padded_width == 0);
    std::vector<int64_t> result;
    result.reserve(encoded.length() / padded_width);
    for (size_t i = 0; i < encoded.length(); i += padded_width) {
        result.push_back(std::stol(encoded.substr(i, padded_width)));
    }
    return result;
}

std::map<char, int> SuccinctGraphSerde::alphabet_char2pos =
    SuccinctGraphSerde::init_map();

std::map<char, int> SuccinctGraphSerde::init_map() {
    alphabet_char2pos.clear();
    for (size_t i = 0; i < ENCODE_ALPHABET.length(); ++i) {
        alphabet_char2pos[ENCODE_ALPHABET.at(i)] = i;
    }
    assert(alphabet_char2pos.size() == SIZE_ENCODE_ALPHABET);
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

std::string SuccinctGraphSerde::encode_int64(int64_t x, int padded_width) {
    assert(ENCODE_ALPHABET.length() == SIZE_ENCODE_ALPHABET);
    std::string res((size_t) padded_width, '0');
    int i = padded_width - 1;
    while (x != 0) {
        res[i] = ENCODE_ALPHABET[x % SIZE_ENCODE_ALPHABET];
        x /= SIZE_ENCODE_ALPHABET;
        --i;
    }
    assert(i >= 0);
    return res;
}

int64_t SuccinctGraphSerde::decode_int64(const std::string& encoded) {
    int64_t res = 0;
    int len = encoded.size();
    for (int i = 0; i < len; ++i) {
        res = res * SIZE_ENCODE_ALPHABET +
            alphabet_char2pos.at(encoded[i]);
    }
    return res;
}

std::string SuccinctGraphSerde::encode_int32(int32_t x, int padded_width) {
    assert(ENCODE_ALPHABET.length() == SIZE_ENCODE_ALPHABET);
    std::string res((size_t) padded_width, '0');
    int i = padded_width - 1;
    while (x != 0) {
        res[i] = ENCODE_ALPHABET[x % SIZE_ENCODE_ALPHABET];
        x /= SIZE_ENCODE_ALPHABET;
        --i;
    }
    assert(i >= 0);
    return res;
}

int32_t SuccinctGraphSerde::decode_int32(const std::string& encoded) {
    int32_t res = 0;
    int len = encoded.size();
    for (int i = 0; i < len; ++i) {
        res = res * SIZE_ENCODE_ALPHABET +
            alphabet_char2pos.at(encoded[i]);
    }
    return res;
}

std::vector<int64_t>
SuccinctGraphSerde::decode_multi_int64(const std::string& encoded,
                                       int padded_width) {
    int len = encoded.length();
    assert(len >= padded_width);
    if (len > padded_width) assert(len % padded_width == 0);

    std::vector<int64_t> res;
    for (int i = 0; i < len; i += padded_width) {
        res.push_back(decode_int64(encoded.substr(i, padded_width)));
    }
    return res;
}

std::vector<int64_t>
SuccinctGraphSerde::unpad_multi_int64(const std::string& encoded) {
    // impl detail: assumes node ids 64 bits
    assert(encoded.length() % WIDTH_NODE_ID_PADDED == 0);
    std::vector<int64_t> result;
    result.reserve(encoded.length() / WIDTH_NODE_ID_PADDED);
    for (size_t i = 0; i < encoded.length(); i += WIDTH_NODE_ID_PADDED) {
        result.push_back(std::stol(encoded.substr(i, WIDTH_NODE_ID_PADDED)));
    }
    return result;
}
