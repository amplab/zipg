#include <iostream>
#include <fstream>
#include <unistd.h>

#include "../include/succinct/SuccinctShard.hpp"

int main(int argc, char **argv) {
    std::string inputpath = std::string(argv[1]);
    int linenum = atoi(argv[2]);
    std::string searchkey = argv[3];

    std::ifstream input(inputpath);
    uint32_t num_keys = std::count(std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>(), '\n');

    SuccinctShard *fd = new SuccinctShard(0, inputpath, num_keys, true, 32, 128);

    // Serialize and save to file
    std::ofstream s_out(inputpath + ".succinct");
    fd->serialize(s_out);
    s_out.close();

    std::string out;
    fd->get(out, linenum);
    printf("line %d: %s\n", linenum, out.c_str());

    std::set<int64_t> res;
    fd->search(res, searchkey);
    for (int64_t i: res)
        printf("%lli\n", i);
}

