#!/bin/bash
set -e

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
neo4j_dir=${HOME_DIR}/benchmark/neo4j
classpath=target/scala-2.10/succinctgraph-assembly-0.1.0-SNAPSHOT.jar

${HOME_DIR}/sbt/sbt assembly

for num_nodes in ${nodes[@]}
do
    echo "Benching nodes: ${num_nodes}"
    # sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    # java -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP} -cp ${classpath} \
       # edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighbor neighbor-latency \
       # ${NEO4J_DIR}/${num_nodes} \
       # ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
       # ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
       # ${HOME_DIR}/neo4j_${num_nodes}_neighbor_latency.txt \
       # ${neo4j_warmup_neighbor} \
       # ${neo4j_measure_neighbor}

    # sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    # java -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP} -cp ${classpath} \
       # edu.berkeley.cs.succinctgraph.neo4jbench.BenchNode node-latency \
       # ${NEO4J_DIR}/${num_nodes} \
       # ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
       # ${QUERY_DIR}/node_query_${num_nodes}.txt \
       # ${HOME_DIR}/neo4j_${num_nodes}_node_latency.txt \
       # ${neo4j_warmup_node} \
       # ${neo4j_measure_node}

    # sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    # java -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP} -cp ${classpath} \
       # edu.berkeley.cs.succinctgraph.neo4jbench.MixBench latency \
       # ${NEO4J_DIR}/${num_nodes} \
       # ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
       # ${QUERY_DIR}/node_query_${num_nodes}.txt \
       # ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
       # ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
       # ${HOME_DIR}/neo4j_${num_nodes}_mix_latency.txt \
       # ${neo4j_warmup_mix} \
       # ${neo4j_measure_mix}

    # sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    # java -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP} -cp ${classpath} \
       # edu.berkeley.cs.succinctgraph.neo4jbench.BenchNode node-node-latency \
       # ${NEO4J_DIR}/${num_nodes} \
       # ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
       # ${QUERY_DIR}/node_query_${num_nodes}.txt \
       # ${HOME_DIR}/neo4j_${num_nodes}_node_node_latency.txt \
       # ${neo4j_warmup_node} \
       # ${neo4j_measure_node}

    # sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    # java -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP} -cp ${classpath} \
        # edu.berkeley.cs.succinctgraph.neo4jbench.NeighborNodeBench latency \
		# ${NEO4J_DIR}/${num_nodes} \
		# ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
		# ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
		# ${HOME_DIR}/neo4j_${num_nodes}_neighbor_node_latency.txt \
		# ${neo4j_warmup_neighbor_node} \
		# ${neo4j_measure_neighbor_node}

    # sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    java -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP} -cp ${classpath} \
        edu.berkeley.cs.succinctgraph.neo4jbench.NeighborNodeBench latency-index \
		${NEO4J_DIR}/${num_nodes} \
		${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
		${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
		${HOME_DIR}/neo4j_${num_nodes}_neighbor_node_latency_index.txt \
		${neo4j_warmup_neighbor_node} \
		${neo4j_measure_neighbor_node}
done
