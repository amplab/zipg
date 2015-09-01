#include "succinct-graph/KeepInputSuccinctFile.h"
#include "succinct-graph/utils.h"

KeepInputSuccinctFile::KeepInputSuccinctFile(
    const std::string& filename,
    SuccinctMode s_mode,
    uint32_t sa_sampling_rate,
    uint32_t npa_sampling_rate)
{
    // Read and keep the input in `raw_input_`
    size_t raw_input_size = read_file(raw_input_, filename);

    COND_LOG_E("raw input size %d, sa %d, isa %d, npa %d\n",
        raw_input_size,
        sa_sampling_rate,
        std::min(SPARSE_ISA_SR, raw_input_size / 2),
        npa_sampling_rate);

    // Depending on `s_mode`, either loads or constructs
    succinct_file_ = new SuccinctFile(filename, s_mode, sa_sampling_rate,
        // Note: very sparse ISA sampling rate
        std::min(SPARSE_ISA_SR, raw_input_size / 2),
        npa_sampling_rate);
}

int64_t KeepInputSuccinctFile::skipping_extract_until(
    uint64_t& suf_arr_idx, // unused
    uint64_t offset, char end_char)
{
    auto iter = raw_input_.begin() + offset;
    uint64_t k = offset + 1;
    while (*iter != end_char) {
        ++iter;
        ++k;
    }
    return k;
}

int64_t KeepInputSuccinctFile::extract_until(
    std::string& result, uint64_t offset, char end_char)
{
    result.clear();
    auto iter = raw_input_.begin() + offset;
    uint64_t k = offset + 1;
    while (*iter != end_char) {
        result += *iter;
        ++iter;
        ++k;
    }
    return k;
}
