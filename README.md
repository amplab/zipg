# succinct-graph
Compressed graph datastore on top of Succinct.

# Build
To build the benchmark driver `bin/bench`, do:
```
make clean && SGFLAGS=-DLOG_DEBUG make -j && make bench
```

To build the RPC component (which depends on Thrift), do `make rpc`. Note that this target is not yet correctly set up for parallel build. To successfully build Thrift for the first time on EC2, some prereqs need to be installed first: run `bash scripts/ec2-prereqs.sh`.

# Quickstart: API
TODO: docs

# Partitioning an input graph
TODO: docs

# Starting a cluster
The list of server hostnames should be put in `conf/hosts`. The master node should be able to SSH into them passwordless.

Configure stuff in `conf/succinct-env.sh` and/or `sbin/succinct-config.sh`.

On the master node, run 
```
make bench
sbin/partition-input.sh && sbin/start-all.sh
```
This would start the cluster (aggregators and worker shards). Then you could launch a benchmark by using `bin/bench`; see `scripts/rates-bench.sh` for an example.

To stop the cluster, run
```
sbin/stop-all.sh
```
