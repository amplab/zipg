#include "partitioners.hpp"

#include <fstream>
#include <random>
#include <memory>

using std::shared_ptr;

class PartitionedGraphFormatter {
public:

    void coalescing_gen_assoc_shards(
        const std::vector<std::string>& input_parts,
        char edge_inner_delim,
        char edge_end_delim,
        const int num_atype,
        const int num_shards,
        const int bytes_per_attr,
        const std::string& attr_file,
        const std::string& output_file_prefix);

};
