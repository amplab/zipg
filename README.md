# succinct-graph
Compressed graph datastore on top of Succinct.

# Build
To build the benchmark driver `bin/bench`, do:
```
make clean && SGFLAGS=-DLOG_DEBUG make -j && make -j bench
```

To build the RPC component (which depends on Thrift), do `make graph-server`.

To build Thrift for the first time, do `make -j build-thrift`. On EC2, some prereqs need to be installed first in order to build Thrift successfully: run `bash scripts/ec2-prereqs.sh`.
