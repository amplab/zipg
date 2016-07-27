#include "succinct_file.h"
#include "succinct_shard.h"

int main(int argc, char** argv) {
  if (argc != 6) {
    fprintf(stderr,
            "Usage: %s [sa-sr] [isa-sr] [npa-sr] [node-file] [edge-file]\n",
            argv[0]);
    return -1;
  }

  uint32_t sa_sr = std::stoll(argv[1]);
  uint32_t isa_sr = std::stoll(argv[2]);
  uint32_t npa_sr = std::stoll(argv[3]);
  std::string node_file = std::string(argv[4]);
  std::string edge_file = std::string(argv[5]);

  SuccinctShard shard(0, node_file, SuccinctMode::CONSTRUCT_IN_MEMORY, sa_sr,
                      isa_sr, npa_sr);
  shard.Serialize();

  SuccinctFile file(edge_file, SuccinctMode::CONSTRUCT_IN_MEMORY, sa_sr, isa_sr,
                    npa_sr);
  file.Serialize();

  return 0;
}

