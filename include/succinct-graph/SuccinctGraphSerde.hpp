#ifndef SUCCINCT_GRAPH_SERDE_H
#define SUCCINCT_GRAPH_SERDE_H

#include <cstdint>
#include <map>
#include <string>
#include <vector>

// SerDe that should adhere to the private typedefs in SuccinctGraph.
class SuccinctGraphSerde {
public:

    /********** padding: left-pad with 0 **********/

    static std::string pad_node_id(int64_t x);
    static std::string pad_atype(int64_t x);
    static std::string pad_edge_width(int32_t x);
    static std::string pad_data_width(int64_t x);

    /********** encoding: encode into alphabet & left-pad with 0 **********/

    static std::string encode_timestamp(int32_t timestamp);
    static int32_t decode_timestamp(const std::string& encoded);

    static std::string encode_node_id(int64_t node_id);
    static int64_t decode_node_id(const std::string& encoded);

    static std::vector<int64_t> decode_multi_int64(const std::string& encoded,
                                                   int padded_width);

    // Widths of padded fields.
    const static int WIDTH_NODE_ID_PADDED = 20;
    const static int WIDTH_ATYPE_PADDED = 20;
    const static int WIDTH_EDGE_WIDTH_PADDED = 10;
    const static int WIDTH_DATA_WIDTH_PADDED = 20;

    // Widths of encoded fields.
    // Rule: (alphabetSize)^width > 2^32 (or 2^64, respectively)
    static const int WIDTH_TIMESTAMP = 6; // encoded in str alphabet of size 64
    static const int WIDTH_NODE_ID = 11; // encoded in str alphabet of size 64

private:

    // Left-pads with zeros to 10 chars (with alphabet 0-9).
    static std::string pad_int32(int32_t x);

    // Left-pads with zeros to 20 chars (with alphabet 0-9).
    static std::string pad_int64(int64_t x);

    // Encodes decimal input to base alphabet, optionally padding the result
    // according to padded_width (with leading 0).
    static std::string encode_int64(int64_t x, int padded_width);

    // Decodes from base alphabet to decimal, treating as a single encoded elem.
    static int64_t decode_int64(const std::string& encoded);

    static std::string encode_int32(int32_t x, int padded_width);
    static int32_t decode_int32(const std::string& encoded);

    const static std::string ENCODE_ALPHABET;
    const static int SIZE_ENCODE_ALPHABET;

    static std::map<char, int> init_map();
    static std::map<char, int> alphabet_char2pos;

};

#endif
