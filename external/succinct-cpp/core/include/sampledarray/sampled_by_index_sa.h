#ifndef SAMPLED_BY_INDEX_SA_H
#define SAMPLED_BY_INDEX_SA_H

#include "flat_sampled_array.h"

class SampledByIndexSA : public FlatSampledArray {
 public:
  // Constructor
  SampledByIndexSA(uint32_t sampling_rate, NPA *npa, ArrayStream& sa_stream,
                   uint64_t sa_n, SuccinctAllocator &s_allocator);

  SampledByIndexSA(uint32_t sampling_rate, NPA *npa,
                   SuccinctAllocator &s_allocator);

  // Access element at index i
  virtual uint64_t operator[](uint64_t i);

  virtual size_t MemoryMap(std::string filename) {
    uint8_t *data_buf, *data_beg;
    data_buf = data_beg = (uint8_t *) SuccinctUtils::MemoryMapUnpopulated(
        filename);

    data_size_ = *((uint64_t *) data_buf);
    data_buf += sizeof(uint64_t);
    data_bits_ = *((uint8_t *) data_buf);
    data_buf += sizeof(uint8_t);
    original_size_ = *((uint64_t *) data_buf);
    data_buf += sizeof(uint64_t);
    sampling_rate_ = *((uint32_t *) data_buf);
    data_buf += sizeof(uint32_t);

    data_buf += SuccinctBase::MemoryMapBitmap(&data_, data_buf);

    return data_buf - data_beg;
  }

 protected:
  // Sample original SA by index
  virtual void Sample(ArrayStream& sa_stream, uint64_t n);
};

#endif
