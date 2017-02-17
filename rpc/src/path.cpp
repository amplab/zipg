#include "succinct_graph_types.h"

bool Path::operator<(Path const& other) const {
  return std::pair<int64_t, int64_t>(src, dst)
      < std::pair<int64_t, int64_t>(other.src, other.dst);
}
