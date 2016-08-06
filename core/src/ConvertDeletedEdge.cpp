#include <vector>
#include <fstream>
#include <cstdio>
#include <cstdint>
#include <cstdlib>

#include "bitmap.h"
#include "DeletedEdges.h"

int main(int argc, char** argv) {
  std::string filename = argv[1];

  std::ifstream in(filename);
  std::ofstream out(filename + ".new");
  size_t num_edge_records;
  in.read(reinterpret_cast<char *>(&num_edge_records), sizeof(size_t));
  std::vector<int64_t> offsets;
  std::vector<DeletedEdges::edge_record_id> edge_record_ids;
  int64_t cur_offset = 0;
  std::cout << "Reading " << num_edge_records << "from file " << filename
            << "\n";
  for (size_t i = 0; i < num_edge_records; i++) {
    int64_t src, atype;
    in.read(reinterpret_cast<char *>(&src), sizeof(int64_t));
    in.read(reinterpret_cast<char *>(&atype), sizeof(int64_t));
    bitmap::Bitmap *b = new bitmap::Bitmap();
    size_t num_edges = b->Deserialize(in);

    DeletedEdges::edge_record_id rec = { src, atype };
    edge_record_ids.push_back(rec);
    offsets.push_back(cur_offset);
    cur_offset += num_edges;
    delete b;
  }

  std::cout << "Total number of edges = " << cur_offset << "\n";

  DeletedEdges del(edge_record_ids, offsets, cur_offset);

  std::cout << "Created deleted edges with " << del.GetNumEdges()
            << " edges and " << del.GetNumRecords() << " edges.\n";
  del.Serialize(out);

  return 0;
}
