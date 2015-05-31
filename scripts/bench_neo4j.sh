#!/bin/bash
SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
neo4j_dir=${HOME_DIR}/benchmark/neo4j
classpath=".:${neo4j_dir}:${neo4j_dir}/lib/neo4j-kernel-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-primitive-collections-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-io-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-lucene-index-2.2.0-RC01.jar:${neo4j_dir}/lib/lucene-core-3.6.2.jar"
javac -cp ${classpath} ${neo4j_dir}/*.java
for num_nodes in ${nodes[@]}
do
    echo "Benching nodes: ${num_nodes}"
    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    java -XX:+UseConcMarkSweepGC -Xmx3000m -cp ${classpath} BenchNeighbor neighbor-latency ${NEO4J_DIR}/${num_nodes} ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt ${QUERY_DIR}/neighbor_query_${num_nodes}.txt ${HOME_DIR}/neo4j_${num_nodes}_neighbor_latency.txt ${neo4j_warmup_neighbor} ${neo4j_measure_neighbor}
    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    java -XX:+UseConcMarkSweepGC -Xmx3000m -cp ${classpath} BenchNode node-latency ${NEO4J_DIR}/${num_nodes} ${QUERY_DIR}/node_warmup_${num_nodes}.txt ${QUERY_DIR}/node_query_${num_nodes}.txt ${HOME_DIR}/neo4j_${num_nodes}_node_latency.txt ${neo4j_warmup_node} ${neo4j_measure_node}
    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    java -XX:+UseConcMarkSweepGC -Xmx3000m -cp ${classpath} MixBench latency ${NEO4J_DIR}/${num_nodes} ${QUERY_DIR}/node_warmup_${num_nodes}.txt ${QUERY_DIR}/node_query_${num_nodes}.txt ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt ${QUERY_DIR}/neighbor_query_${num_nodes}.txt ${HOME_DIR}/neo4j_${num_nodes}_mix_latency.txt ${neo4j_warmup_mix} ${neo4j_measure_mix}
    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    java -XX:+UseConcMarkSweepGC -Xmx3000m -cp ${classpath} BenchNode node-node-latency ${NEO4J_DIR}/${num_nodes} ${QUERY_DIR}/node_warmup_${num_nodes}.txt ${QUERY_DIR}/node_query_${num_nodes}.txt ${HOME_DIR}/neo4j_${num_nodes}_node_node_latency.txt ${neo4j_warmup_node} ${neo4j_measure_node}
    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    java -XX:+UseConcMarkSweepGC -Xmx3000m -cp ${classpath} NeighborNodeBench latency ${NEO4J_DIR}/${num_nodes} ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt ${HOME_DIR}/neo4j_${num_nodes}_neighbor_node_latency.txt ${neo4j_warmup_neighbor_node} ${neo4j_measure_neighbor_node}
done
