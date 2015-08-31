#include "succinct-graph/KeepInputSuccinctFile.h"
#include "succinct-graph/utils.h"

KeepInputSuccinctFile::KeepInputSuccinctFile(
    const std::string& filename,
    SuccinctMode s_mode,
    uint32_t sa_sampling_rate,
    uint32_t npa_sampling_rate)
{
    // Read and keep the input in `raw_input_`
    size_t file_size = read_file(raw_input_, filename);

    // Depending on `s_mode`, either loads or constructs
    succinct_file_ = new SuccinctFile(filename, s_mode, sa_sampling_rate,
        // Note: very sparse ISA sampling rate
        std::min(sparse_isa_sampling_rate_, file_size / 2),
        npa_sampling_rate);
}
