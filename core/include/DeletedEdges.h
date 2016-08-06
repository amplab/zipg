#ifndef DELETEDEDGES_H_
#define DELETEDEDGES_H_

#include <unistd.h>
#include <cstdlib>
#include <cstdio>
#include <tuple>
#include <cassert>

class DeletedEdges {
 public:
  struct edge_record_id {
    int64_t src;
    int64_t atype;

    bool operator<(const struct edge_record_id& rhs) {
      return std::tie(src, atype) < std::tie(rhs.src, rhs.atype);
    }

    bool operator<=(const struct edge_record_id& rhs) {
      return std::tie(src, atype) <= std::tie(rhs.src, rhs.atype);
    }

    bool operator>(const struct edge_record_id& rhs) {
      return std::tie(src, atype) > std::tie(rhs.src, rhs.atype);
    }

    bool operator>=(const struct edge_record_id& rhs) {
      return std::tie(src, atype) >= std::tie(rhs.src, rhs.atype);
    }

    bool operator==(const struct edge_record_id& rhs) {
      return std::tie(src, atype) == std::tie(rhs.src, rhs.atype);
    }
  };

  DeletedEdges() {
    bitmap_ = NULL;
    record_ids_ = NULL;
    offsets_ = NULL;
    num_entries_ = 0;
    num_edges_ = 0;
  }

  DeletedEdges(size_t num_entries, size_t num_edges) {
    bitmap_ = new bitmap::Bitmap(num_edges);
    record_ids_ = new edge_record_id[num_entries];
    offsets_ = new int64_t[num_entries];
    num_entries_ = num_entries;
    num_edges_ = num_edges;
  }

  DeletedEdges(std::vector<edge_record_id> record_ids, std::vector<int64_t> offsets, int64_t num_edges) {
    assert(record_ids.size() == offsets.size());
    bitmap_ = new bitmap::Bitmap(num_edges);
    record_ids_ = &record_ids[0];
    offsets_ = &offsets[0];
    num_entries_ = record_ids.size();
    num_edges_ = num_edges;
  }

  bool IsDeleted(int64_t src, int64_t atype, int64_t edge_idx) {
    int64_t off = offsets_[FindRecordIdx(src, atype)];
    return bitmap_->GetBit(off + edge_idx);
  }

  void Delete(int64_t src, int64_t atype, int64_t edge_idx) {
    int64_t off = offsets_[FindRecordIdx(src, atype)];
    bitmap_->SetBit(off + edge_idx);
  }

  size_t Serialize(std::ostream& out) {
    size_t out_size = 0;

    out.write(reinterpret_cast<const char*>(&num_entries_), sizeof(int64_t));
    out_size += sizeof(int64_t);

    out.write(reinterpret_cast<const char*>(&num_edges_), sizeof(int64_t));
    out_size += sizeof(int64_t);

    out.write(reinterpret_cast<const char*>(record_ids_),
              num_entries_ * sizeof(edge_record_id));
    out_size += (sizeof(edge_record_id) * num_entries_);

    out.write(reinterpret_cast<const char*>(offsets_),
              num_entries_ * sizeof(int64_t));
    out_size += (sizeof(int64_t) * num_entries_);

    out_size += bitmap_->Serialize(out, 0);

    return out_size;
  }

  size_t Deserialize(std::istream& in) {
    size_t in_size = 0;

    in.read(reinterpret_cast<char *>(&num_entries_), sizeof(int64_t));
    in_size += sizeof(int64_t);

    in.read(reinterpret_cast<char *>(&num_edges_), sizeof(int64_t));
    in_size += sizeof(int64_t);

    record_ids_ = new edge_record_id[num_entries_];
    in.read(reinterpret_cast<char *>(record_ids_),
            sizeof(edge_record_id) * num_entries_);
    in_size += (sizeof(edge_record_id) * num_entries_);

    offsets_ = new int64_t[num_entries_];
    in.read(reinterpret_cast<char *>(offsets_), sizeof(int64_t) * num_entries_);
    in_size += (sizeof(edge_record_id) * num_entries_);

    bitmap_ = new bitmap::Bitmap();
    in_size += bitmap_->Deserialize(in);

    return 0;
  }

  size_t MemoryMap(uint8_t* buf) {
    uint8_t *data, *data_beg;
    data = data_beg = buf;

    num_entries_ = *((uint64_t *) data);
    data += sizeof(uint64_t);

    num_edges_ = *((uint64_t *) data);
    data += sizeof(uint64_t);

    record_ids_ = (edge_record_id *) data;
    data += (sizeof(edge_record_id) * num_entries_);

    offsets_ = (int64_t*) data;
    data += (sizeof(int64_t) * num_entries_);

    bitmap_ = new bitmap::Bitmap();
    data += bitmap_->MemoryMap(data);

    return data - data_beg;
  }

 private:
  int64_t FindRecordIdx(int64_t src, int64_t atype) {
    edge_record_id rec = { src, atype };
    // Binary search for the offset in the list of value offsets.
    uint32_t lo = 0, hi = num_entries_ - 1;
    while (lo < hi) {
      uint32_t mid = lo + (hi - lo) / 2;
      edge_record_id val = record_ids_[mid];
      if (val <= rec)
        lo = mid + 1;
      else
        hi = mid;
    }

    return lo - 1;
  }

  // One big bitmap
  bitmap::Bitmap* bitmap_;
  edge_record_id* record_ids_;
  int64_t* offsets_;
  size_t num_entries_;
  size_t num_edges_;
};

#endif /* DELETEDEDGES_H_ */
