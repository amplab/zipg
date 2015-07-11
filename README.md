# succinct-graph
Compressed graph datastore on top of Succinct.

# Build
To build the benchmark driver `bin/bench`, do:
```
make clean && SGFLAGS=-DLOG_DEBUG make -j && make -j bench
```

To build the RPC component (which depends on Thrift), do `make rpc`. Note that this target is not yet correctly set up for parallel build.

To build Thrift for the first time, do `make -j build-thrift` in `external/succinct-cpp`. On EC2, some prereqs need to be installed first in order to build Thrift successfully: run `bash scripts/ec2-prereqs.sh`.

# Starting a cluster
The list of server hostnames should be put in `conf/hosts`. The master node should be able to SSH into them passwordless.

Configure stuff in `conf/succinct-env.sh` and/or `sbin/succinct-config.sh`.

On the master node, run 
```
sbin/partition-input.sh && sbin/start-succinct.sh 
make sharded-bench && bin/sharded-graph-bench
```

To cleanly stop the cluster, run
```
sbin/stop-succinct.sh
```
