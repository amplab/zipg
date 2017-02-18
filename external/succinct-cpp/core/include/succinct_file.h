#ifndef SUCCINCT_H
#define SUCCINCT_H

#include <cstdint>
#include <string>
#include <cstring>
#include <vector>
#include <set>

#include "succinct_core.h"
#include "regex/regex.h"

class SuccinctFile : public SuccinctCore {
 public:
  SuccinctFile(std::string filename, SuccinctMode s_mode =
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

  /*
   * Get the name of the SuccinctFile
   */
  std::string Name();

  /*
   * Random access into the Succinct file with the specified offset
   * and length.  Clears `result` for caller.
   */
  void Extract(std::string& result, uint64_t offset, uint64_t len);

  /*
   * Get the count of a string in the Succinct file
   */
  uint64_t Count(const std::string& str);

  /*
   * Get the offsets of all the occurrences
   * of a string in the Succinct file
   */
  void Search(std::vector<int64_t>& result, const std::string& str);

  int64_t Search(const std::string& str);

  /*
   * Get the offsets corresponding to matches of regex.
   */
  void RegexSearch(std::set<std::pair<size_t, size_t>>& results, const std::string& query);

  /*********** SuccinctGraph-specific optimizations ***********/
  // TODO: clean these up

  // Clears `result` for caller.  Use suf_arr_idx as initial index and avoids
  // initial ISA lookup.  Further, store the last updated index back as well.
  void Extract(
      std::string& result,
      uint64_t& suf_arr_idx,
      uint64_t offset, // unused
      uint64_t len);

  void ExtractApprox(std::string& result, uint64_t start_off_approx, char start, char end);

  // Starts extraction at `offset` until hitting `end_char`.  Returns the
  // next offset.  If `suf_arr_idx` is not -1, use it and avoid initial
  // ISA lookup.  Upon success, puts next suf arr idx back into it as well.
  int64_t SkippingExtractUntil(
      uint64_t& suf_arr_idx, uint64_t offset, char end_char);

  // Clears `result` for caller, and puts everything up to (not including)
  // `end_char`.  Returns the next offset.
  int64_t ExtractUntil(std::string& result, uint64_t offset, char end_char);

  // If `suf_arr_idx` is -1ULL, use `offset` to perform initial ISA lookup;
  // otherwise just use it.  On exit, puts the next suffix array index back
  // into `suf_arr_idx`, and returns the next offset.  Clears `result` for the
  // caller upon entry.
  int64_t ExtractUntil(
      std::string& result,
      uint64_t& suf_arr_idx,
      uint64_t offset,
      char end_char);

  // END: SuccinctGraph-specific optimizations

 private:
  // std::pair<int64_t, int64_t> GetRangeSlow(const char *str, uint64_t len);
  std::pair<int64_t, int64_t> GetRange(const char *str, uint64_t len);
  uint64_t ComputeContextValue(const char *str, uint64_t pos);

  std::string input_filename_;
  std::string succinct_filename_;

};

#endif
