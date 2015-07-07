#include "succinct-graph/partitioners.hpp"

void RangePartitioner::partition(
    const std::string& node_file_in,
    const std::string& edge_file_in)
{
    auto num_digits = [](int32_t number) {
       if (number == 0) return 1;
       int32_t digits = 0;
       while (number != 0) {
           number /= 10;
           ++digits;
       }
       return digits;
    };

    auto format_out_name = [](const std::string& prefix, int digits, int num) {
        char s[digits + 1];
        sprintf(s, "%0*d", digits, num);
        return std::string(prefix + "-part" + s);
    };

    std::ifstream node_file_stream(node_file_in);
    std::ofstream curr_split_ofstream;
    std::string line, curr_split_filename;

    std::vector<std::string> lines;
    while (std::getline(node_file_stream, line)) {
        lines.push_back(line);
    }

    assert(this->num_shards_ <= lines.size()); // we can relax this assumption
    int lines_per_split = lines.size() / this->num_shards_;
    int num_shards_digits = num_digits(this->num_shards_);
    int diff = lines.size() - lines_per_split * this->num_shards_;
    int shard_idx = 0;

    int split_start_line = 0;
    while (split_start_line < lines.size()) {
        std::string s(format_out_name(
            node_file_in, num_shards_digits, shard_idx));
        std::ofstream curr_split_ofstream(s);
        int j = split_start_line;
        for ( ; j < split_start_line + lines_per_split && j < lines.size(); ++j)
            curr_split_ofstream << lines.at(j) << std::endl;

        // first diff shards output one more line
        if (shard_idx < diff) {
            ++split_start_line;
            curr_split_ofstream << lines.at(j) << std::endl;
        }
        split_start_line += lines_per_split;
        ++shard_idx;
    }

    /*********** edge file ***********/

    std::ifstream edge_file_stream(edge_file_in);
    int64_t curr_id_limit = lines_per_split;
    if (diff > 0) ++curr_id_limit;

    std::string src_id_str;
    shard_idx = 0;
    curr_split_ofstream = std::ofstream(format_out_name(
        edge_file_in, num_shards_digits, 0));

    while (std::getline(edge_file_stream, line)) {
        std::stringstream ss(line);
        std::getline(ss, src_id_str, ' ');
        while (std::stol(src_id_str) >= curr_id_limit) {
            // initialize new stream
            ++shard_idx;
            curr_id_limit += lines_per_split;
            if (shard_idx < diff) ++curr_id_limit; // leftovers
            if (std::stol(src_id_str) < curr_id_limit)
                curr_split_ofstream = std::ofstream(format_out_name(
                    edge_file_in, num_shards_digits, shard_idx));
        }
        curr_split_ofstream << line << std::endl;
    }
}

int main() {
    RangePartitioner partitioner(31);
    partitioner.partition("Makefile", "data/assocs/test.assoc");
}
