#ifndef SUCCINCT_SHARD_H
#define SUCCINCT_SHARD_H

#include <cstdint>
#include <string>
#include <cstring>
#include <vector>
#include <set>

#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <string>
#include <fstream>
#include <streambuf>

#include "utils.h"
#include "regex/regex.h"
#include "succinct_core.h"

class SuccinctShard : public SuccinctCore {
 public:
  static const int64_t MAX_KEYS = 1L << 32;

  SuccinctShard(uint32_t id, std::string datafile, SuccinctMode s_mode =
                    SuccinctMode::CONSTRUCT_IN_MEMORY,
                uint32_t sa_sampling_rate = 32, uint32_t isa_sampling_rate = 32,
                uint32_t npa_sampling_rate = 128,
                SamplingScheme sa_sampling_scheme =
                    SamplingScheme::FLAT_SAMPLE_BY_INDEX,
                SamplingScheme isa_sampling_scheme =
                    SamplingScheme::FLAT_SAMPLE_BY_INDEX,
                NPA::NPAEncodingScheme npa_encoding_scheme =
                    NPA::NPAEncodingScheme::ELIAS_GAMMA_ENCODED,
                uint32_t context_len = 3, uint32_t sampling_range = 1024);

  virtual ~SuccinctShard() {
  }

  uint32_t GetSASamplingRate();

  uint32_t GetISASamplngRate();

  uint32_t GetNPASamplingRate();

  std::string Name();

  size_t GetNumKeys();

  bool Delete(int64_t key);

  // Clears `result` for caller.
  void Get(std::string& result, int64_t key);

  // Clears `result` for caller.
  void Access(std::string& result, int64_t key, int32_t offset, int32_t len);

  int64_t Count(const std::string& str);

  // Does not clear `result` for caller.
  void Search(std::set<int64_t>& result, const std::string& str);

  int64_t FlatCount(const std::string& str);

  void FlatSearch(std::vector<int64_t>& result, const std::string& str);

  void FlatExtract(std::string& result, int64_t offset, int64_t len);

  void RegexSearch(std::set<std::pair<size_t, size_t>>& result,
                   const std::string& str, bool opt = true);

  void RegexCount(std::vector<size_t>& result, const std::string& str);

  /*********** SuccinctGraph-specific optimizations & changes ***********/

  // Starting from a *raw* file offset, extract each char & compare against
  // search_key until hitting the end_char or first difference.
  bool ExtractCompareUntil(
      int64_t raw_offset,
      char end_char,
      const std::string& compare_key);

  void ExtractUntil(
    std::string& result,
    int64_t raw_offset,
    char end_char);

  // Clears `result` for caller.  Reuses `suf_arr_idx` if not -1; otherwise,
  // lookup record start for `key`.
  //
  // On success, puts next suffix array offset after `end_char` back; if
  // additionally `suf_arr_idx` was passed in as -1, returns the offset
  // pointing to next char (otherwise the returned result is
  // not meaningful).
  //
  // On error, returns -1.
  int64_t ExtractUntil(
      std::string& result,
      uint64_t& suf_arr_idx,
      int64_t key,
      char end_char);

  // Starts extraction at `raw_offset` until hitting `end_char`.  If
  // `suf_arr_idx` is not -1, use it to avoid the initial ISA lookup that
  // uses `raw_offset_`.  On success, puts back the corresponding suffix array
  // index (right after `end_char`) into `suf_arr_idx`. Clears `result` for
  // the caller.
  void ExtractUntilHint(
      std::string& result,
      uint64_t& suf_arr_idx,
      int64_t raw_offset,
      char end_char);

  // Unsafe: use these keys without performing any checks.  Caller should
  // make sure the size of the vector matches GetNumKeys(), no concurrent
  // calls, etc.
  void SetKeys(const std::vector<int64_t> keys);

  // END: SuccinctGraph-specific optimizations & changes

  // Serialize succinct data structures
  virtual size_t Serialize();

  // Deserialize succinct data structures
  virtual size_t Deserialize();

  // Memory map succinct data structures
  virtual size_t MemoryMap();

  // Get succinct shard size
  virtual size_t StorageSize();

 protected:
  int64_t GetKeyPos(const int64_t value_offset);
  int64_t GetValueOffsetPos(const int64_t key);

  // std::pair<int64_t, int64_t> get_range_slow(const char *str, uint64_t len);
  std::pair<int64_t, int64_t> GetRange(const char *str, uint64_t len);

  uint64_t ComputeContextValue(const char *str, uint64_t pos);

  std::vector<int64_t> keys_;
  std::vector<int64_t> value_offsets_;
  Bitmap *invalid_offsets_;
  uint32_t id_;
};

#endif
