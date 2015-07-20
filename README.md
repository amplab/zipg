# Succinct Graph
Compressed graph store on top of Succinct.

# Build
To build the benchmark driver `bin/bench`, do:
```
git submodule update --init
make clean && SGFLAGS=-DLOG_DEBUG make -j && make bench
```

To build the RPC component (which depends on Thrift), do `make rpc`. This target is not yet correctly set up for parallel build. The major hassle here is to successfully install the Thrift compiler for the first time: on EC2, some prereqs need to be installed first: run `sudo bash scripts/ec2-prereqs.sh`; if errors occur, try to fix them manually (e.g. don't run the `autoreconf --force --install` command in Succinct's `build-thrift` target).

# Quickstart: API
TODO: docs

# Importing an input graph 
Given an edge list, Succinct Graph provides various formatter utilities to convert it into a ready-to-use format (including randomly producing node/edge attributes from a specified attribute file). See `formatter.sh` for an example usage.

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

# Building benchmarks
The Neo4j benchmark is written in Java and can be built and started as follows:
```
sbt/sbt assembly
bash scripts/bench_neo4j.sh
```
