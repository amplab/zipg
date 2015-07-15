#!/bin/bash
set -e

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
neo4j_dir=${HOME_DIR}/benchmark/neo4j
classpath=target/scala-2.10/succinctgraph-assembly-0.1.0-SNAPSHOT.jar

TOTAL_MEM=249856
AVAIL_MEM=$(( $TOTAL_MEM - 512 )) # reserve 512m for OS

DATASET=higgs-twitter-20attr35each
DATASET=liveJournal-40attr16each

postfix="-20attr35each"
postfix=""

for JVM_HEAP in 4096
do
  # 1st is default neo4j setting; 2nd is "use more"
  for PC in Auto #`echo "0.75 * ($TOTAL_MEM - $JVM_HEAP)" | bc | awk '{printf("%d", $1)}'` #`echo "$AVAIL_MEM - $JVM_HEAP" | bc | awk '{printf("%d", $1)}'`
  do
    echo "Setting -Xmx to ${JVM_HEAP}m"
    echo "Setting neo4j pagecache to ${PC}m"

    for num_nodes in ${nodes[@]}
    do
        echo "Benching nodes: ${num_nodes}"

        sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        java -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighbor neighbor-latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_neighbor_latency_jvm${JVM_HEAP}m_pagecache${PC}m${postfix}.txt \
           ${neo4j_warmup_neighbor} \
           ${neo4j_measure_neighbor} 5g

        sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        java -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.BenchNode node-latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/node_query_${num_nodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_node_latency_jvm${JVM_HEAP}m_pagecache${PC}m${postfix}.txt \
           ${neo4j_warmup_node} \
           ${neo4j_measure_node} 5g #\
           #${PC}m

        sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        java -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.BenchNode node-node-latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/node_query_${num_nodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_node_node_latency_jvm${JVM_HEAP}m_pagecache${PC}m${postfix}.txt \
           ${neo4j_warmup_node} \
           ${neo4j_measure_node} 5g

        sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        java -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.NeighborNodeBench latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
            ${HOME_DIR}/neo4j_${DATASET}_neighbor_node_latency_jvm${JVM_HEAP}m_pagecache${PC}m${postfix}.txt \
            ${neo4j_warmup_neighbor_node} \
            ${neo4j_measure_neighbor_node} 5g

        sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        java -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighborAtype \
            latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
            ${HOME_DIR}/neo4j_${DATASET}_neighborAtype_latency_jvm${JVM_HEAP}m_pagecache${PC}m${postfix}.txt \
            ${neo4j_warmup_neighbor_atype} \
            ${neo4j_measure_neighbor_atype}

        # sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        # java -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            # edu.berkeley.cs.succinctgraph.neo4jbench.NeighborNodeBench latency-index \
            # ${NEO4J_DIR}/${DATASET} \
            # ${QUERY_DIR}/neighbor_node_warmup_${DATASET}.txt \
            # ${QUERY_DIR}/neighbor_node_query_${DATASET}.txt \
            # ${HOME_DIR}/neo4j_${DATASET}_neighbor_node_latency_index.txt \
            # ${neo4j_warmup_neighbor_node} \
            # ${neo4j_measure_neighbor_node}



        # sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        # java -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           # edu.berkeley.cs.succinctgraph.neo4jbench.MixBench latency \
           # ${NEO4J_DIR}/${DATASET} \
           # ${QUERY_DIR}/node_warmup_${DATASET}.txt \
           # ${QUERY_DIR}/node_query_${DATASET}.txt \
           # ${QUERY_DIR}/neighbor_warmup_${DATASET}.txt \
           # ${QUERY_DIR}/neighbor_query_${DATASET}.txt \
           # ${HOME_DIR}/neo4j_${DATASET}_mix_latency.txt \
           # ${neo4j_warmup_mix} \
           # ${neo4j_measure_mix}

    done
  done
done
