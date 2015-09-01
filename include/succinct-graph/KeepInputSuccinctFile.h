#ifndef KEEP_INPUT_SUCCINCT_FILE_H
#define KEEP_INPUT_SUCCINCT_FILE_H

#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "succinct/SuccinctFile.hpp"

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

    inline size_t serialize() {
        return succinct_file_->serialize();
    }

    inline void extract(std::string& result, uint64_t offset, uint64_t len)
    {
        result.assign(raw_input_, offset, len);
    }

    /*********** SuccinctGraph-specific optimizations ***********/

    // NOTE: to ensure compilable (...), these calls have the same signatures
    // as those in SuccinctFile.  However, note that we never use `suf_arr_idx`
    // here, because ISA is not used in answering queries.

    // Clears `result` for caller.
    inline void extract(
        std::string& result,
        uint64_t& suf_arr_idx,
        uint64_t offset,
        uint64_t len)
    {
        result.assign(raw_input_, offset, len);
    }

    // Starts extraction at `offset` until hitting `end_char`.  Returns the
    // next offset.  Upon success, returns next offset.
    int64_t skipping_extract_until(
        uint64_t& suf_arr_idx, uint64_t offset, char end_char);

    // Clears `result` for caller, and puts everything up to (not including)
    // `end_char`.  Returns the next offset.
    int64_t extract_until(std::string& result, uint64_t offset, char end_char);

    // On exit, returns the next offset.  Clears `result` for the caller.
    inline int64_t extract_until(
        std::string& result,
        uint64_t& suf_arr_idx,
        uint64_t offset,
        char end_char)
    {
        return extract_until(result, offset, end_char);
    }

    //**************** END: SuccinctGraph-specific optimizations

    inline void search(std::vector<int64_t>& result, const std::string& str) {
        succinct_file_->search(result, str);
    }

private:
    SuccinctFile* succinct_file_ = nullptr;

    std::string raw_input_;

    // For input of 1TB, this results in a negligible ISA of size 16MB.
    const size_t SPARSE_ISA_SR = 1 << 16;

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
