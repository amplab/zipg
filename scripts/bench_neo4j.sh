#!/bin/bash
set -e

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
neo4j_dir=${HOME_DIR}/benchmark/neo4j
classpath=target/scala-2.10/succinctgraph-assembly-0.1.0-SNAPSHOT.jar

TOTAL_MEM=249856
AVAIL_MEM=$(( $TOTAL_MEM - 512 )) # reserve 512m for OS

DATASET=higgs-twitter-40attr16each
postfix="-40attr16each"

DATASET=higgs-twitter-20attr35each
postfix="-20attr35each"

DATASET=liveJournal-40attr16each
postfix=""

if [[ "$DATASET" == "liveJournal-40attr16each" ]]; then
  pushd ${QUERY_DIR} >/dev/null
  yes | cp -rf liveJournal-40attr16each-queries/*txt ./
  popd >/dev/null
elif [[ "$DATASET" == "higgs-twitter-20attr35each" ]]; then
  pushd ${QUERY_DIR} >/dev/null
  yes | cp -rf 20attr35each-queries/*txt ./
  popd >/dev/null
else
  echo implement query copying for me!
  exit 1
fi

#benchNeighbor=T
benchNeighborAtype=T
#benchNeighborNode=T
#benchNode=T
#benchNodeNode=T
#benchNeighborNodeIndexed=T

for JVM_HEAP in 69632
do
  # 1st is default neo4j setting; 2nd is "use more"
  for PC in Auto #`echo "0.75 * ($TOTAL_MEM - $JVM_HEAP)" | bc | awk '{printf("%d", $1)}'` #`echo "$AVAIL_MEM - $JVM_HEAP" | bc | awk '{printf("%d", $1)}'`
  do
    echo "Setting -Xmx to ${JVM_HEAP}m"
    echo "Setting neo4j pagecache to ${PC}m"

    for num_nodes in ${nodes[@]}
    do
        echo "Benching nodes: ${num_nodes}"

    if [[ -n "$benchNeighbor" ]]; then
        sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighbor neighbor-latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_neighbor_latency_jvm${JVM_HEAP}m_pagecache${PC}m${postfix}.txt \
           ${neo4j_warmup_neighbor} \
           ${neo4j_measure_neighbor} 5g
      fi

    if [[ -n "$benchNode" ]]; then
        sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        #find data/neo4j/${DATASET} -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M \;
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.BenchNode node-latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/node_query_${num_nodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_node_latency_jvm${JVM_HEAP}m_pagecache${PC}m${postfix}.txt \
           ${neo4j_warmup_node} \
           ${neo4j_measure_node} 5g #\
           #${PC}m
      fi

    if [[ -n "$benchNodeNode" ]]; then
        sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.BenchNode node-node-latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/node_query_${num_nodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_node_node_latency_jvm${JVM_HEAP}m_pagecache${PC}m${postfix}.txt \
           ${neo4j_warmup_node} \
           ${neo4j_measure_node} 5g
      fi

    if [[ -n "$benchNeighborNode" ]]; then
        sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.NeighborNodeBench latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
            ${HOME_DIR}/neo4j_${DATASET}_neighbor_node_latency_jvm${JVM_HEAP}m_pagecache${PC}m${postfix}.txt \
            ${neo4j_warmup_neighbor_node} \
            ${neo4j_measure_neighbor_node} 5g
      fi

    if [[ -n "$benchNeighborAtype" ]]; then
        sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighborAtype \
            latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
            ${HOME_DIR}/neo4j_${DATASET}_neighborAtype_latency_jvm${JVM_HEAP}m_pagecache${PC}m${postfix}.txt \
            ${neo4j_warmup_neighbor_atype} \
            ${neo4j_measure_neighbor_atype}
      fi

      if [[ -n "$benchNeighborNodeIndexed" ]]; then
        sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.NeighborNodeBench latency-index \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
            ${HOME_DIR}/neo4j_${DATASET}_neighbor_node_latency_index.txt \
            ${neo4j_warmup_neighbor_node} \
            ${neo4j_measure_neighbor_node}
      fi



        # sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        # java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
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
