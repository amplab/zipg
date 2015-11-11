#include "KVLogStore.h"

#include <string>

int main(int argc, char **argv) {
    // Multistore tests
    KVLogStore kv_log_store("");
    std::string ret;
    kv_log_store.get_value(ret, 0);
    assert(ret == "");
    // kv_log_store.append(0, 0);
}
