#include <fstream>
#include <sstream>
#include <vector>
#include <unistd.h>
#include <stdint.h>

uint64_t Get(const std::string &s, uint32_t n) {
  std::istringstream is(s);
  std::string field;
  do {
    if (!(is >> field))
      return 0;
  } while (n-- != 0);
  return std::stoll(field);
}

int main(int argc, char** argv) {
  if (argc < 2 || argc > 6) {
    fprintf(stderr, "Usage: %s [-k col] [-n nshards] filename\n", argv[0]);
    return -1;
  }

  int c;
  int col_id = 0, num_shards = 1;
  bool line_num = true;
  while ((c = getopt(argc, argv, "k:n:")) != -1) {
    switch (c) {
      case 'k': {
        col_id = std::stoi(optarg);
        line_num = false;
        break;
      }
      case 'n': {
        num_shards = std::stoi(optarg);
        break;
      }
      default: {
        fprintf(stderr, "Error parsing command line args.\n");
      }
    }
  }

  if (optind == argc) {
    fprintf(stderr, "Usage: %s [-k column-index] file-name\n", argv[0]);
    return -1;
  }

  std::string input_file = std::string(argv[optind]);
  std::ifstream in(input_file);
  std::vector<std::ofstream*> out;
  std::string line;
  size_t i = 0;

  for (int i = 0; i < num_shards; i++) {
    std::string partfile = input_file + "." + std::to_string(i);
    std::ofstream* o = new std::ofstream(partfile);
    out.push_back(o);
  }

  while (std::getline(in, line)) {
    if (line_num)
      (*out[i % num_shards]) << line << "\n";
    else
      (*out[Get(line, col_id) % num_shards]) << line << "\n";
    i++;
  }

  for (int i = 0; i < num_shards; i++) {
    out[i]->close();
  }
}
