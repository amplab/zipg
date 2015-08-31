#ifndef KEEP_INPUT_SUCCINCT_FILE_H
#define KEEP_INPUT_SUCCINCT_FILE_H

#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "succinct/SuccinctFile.hpp"

using std::unique_ptr;

// A wrapper over SuccinctFile that does the following:
//
//   (1) Keeps Succinct's SA, NPA, and a very sparsely-sampled ISA
//   (2) Keeps the raw input in unstructured bytes/characters
//
// Hence, search is served by Succinct algorithms using SA and NPA, and extract
// is served directly by the raw input.
class KeepInputSuccinctFile {
public:
    KeepInputSuccinctFile(
        const std::string& filename,
        SuccinctMode s_mode = SuccinctMode::CONSTRUCT_IN_MEMORY,
        uint32_t sa_sampling_rate = 32,
        uint32_t npa_sampling_rate = 128);

    ~KeepInputSuccinctFile() {
        if (succinct_file_ != nullptr) {
            delete succinct_file_;
        }
    }

    size_t serialize();

    inline void search(std::vector<int64_t>& result, const std::string& str) {
        succinct_file_->search(result, str);
    }

private:
    SuccinctFile* succinct_file_ = nullptr;

    std::string raw_input_;

    // For input of 1TB, this results in a negligible ISA of size 16MB.
    size_t sparse_isa_sampling_rate_ = 1 << 16;

    // Clears `contents` for caller.
    inline size_t read_file(std::string& contents, const std::string& filename)
    {
        std::ifstream in(filename, std::ios::in | std::ios::binary);
        if (in) {
            in.seekg(0, std::ios::end);
            contents.resize(in.tellg());
            in.seekg(0, std::ios::beg);
            in.read(&contents[0], contents.size());
            in.close();
            return contents.size();
        }
        return -1;
    }

};

#endif
