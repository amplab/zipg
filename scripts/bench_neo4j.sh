#!/bin/bash
set -e

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
neo4j_dir=${HOME_DIR}/benchmark/neo4j
classpath=target/scala-2.10/succinctgraph-assembly-0.1.0-SNAPSHOT.jar

TOTAL_MEM=249856
TOTAL_MEM=15043 # c4.2x, 15GB
AVAIL_MEM=$(( $TOTAL_MEM - 512 )) # reserve 512m for OS

minDegs=('' '-minDeg30')
minDegs=('')
minDegs=('-minDeg60')
minDegs=('-minDeg30WithTsAttr')

for minDeg in "${minDegs[@]}"; do

DATASET=higgs-twitter-40attr16each
DATASET=higgs-twitter-20attr35each
DATASET=liveJournal-40attr16each${minDeg}

if [[ "$DATASET" == "liveJournal-40attr16each"* ]]; then
  pushd ${QUERY_DIR} >/dev/null
  yes | cp -rf liveJournal-40attr16each${minDeg}-queries/*txt ./
  popd >/dev/null
elif [[ "$DATASET" == "higgs-twitter-20attr35each" ]]; then
  pushd ${QUERY_DIR} >/dev/null
  yes | cp -rf 20attr35each-queries/*txt ./
  popd >/dev/null
else
  echo implement query copying for me!
  exit 1
fi
NEO4J_DIR=/mnt2T/data/neo4j

#benchNeighbor=T
#benchNeighborAtype=T
#benchNeighborNode=T
#benchNode=T
#benchNodeNode=T
#benchNeighborNodeIndexed=T
#benchMixed=T

thputThreads=1
#benchNhbrThput=T

benchAssocRange=T
#benchObjGet=T
#benchAssocGet=T
#benchAssocCount=T
#benchAssocTimeRange=T
benchTAOMix=T

pageCacheForNodes=8543m # works, has tradeoff
pageCacheForNodes=""
pageCacheIgnoreIndexes=72g

#for pageCacheForNodes in 9574m 9370m; do
for JVM_HEAP in 6900; do
  # "use more": #`echo "0.75 * ($TOTAL_MEM - $JVM_HEAP)" | bc | awk '{printf("%d", $1)}'` #`echo "$AVAIL_MEM - $JVM_HEAP" | bc | awk '{printf("%d", $1)}'`
  # For default neo4j setting, use "Auto"; otherwise support custom value with postfix k/m/g
  PC=$pageCacheForNodes
    echo "Setting -Xmx to ${JVM_HEAP}m"

    for num_nodes in ${nodes[@]}
    do

    if [[ -n "$benchNodeNode" ]]; then
        sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        find /mnt2T/data/neo4j/${DATASET}/schema/index -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
        find /mnt2T/data/neo4j/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.BenchNode node-node-latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/node_query_${num_nodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_node_node_latency_jvm${JVM_HEAP}m_pagecache${PC}m.txt \
           ${neo4j_warmup_node} \
           ${neo4j_measure_node} \
           ${pageCacheForNodes}
      fi

    if [[ -n "$benchNode" ]]; then
        sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        find /mnt2T/data/neo4j/${DATASET}/schema/index -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
        find /mnt2T/data/neo4j/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;

        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.BenchNode node-latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/node_query_${num_nodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_node_latency_jvm${JVM_HEAP}m_pagecache${PC}m.txt \
           ${neo4j_warmup_node} \
           ${neo4j_measure_node} \
           ${pageCacheForNodes}
      fi

    if [[ -n "$benchNeighborNode" ]]; then
        sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        find /mnt2T/data/neo4j/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.NeighborNodeBench latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
            ${HOME_DIR}/neo4j_${DATASET}_neighbor_node_latency_jvm${JVM_HEAP}m_pagecache${PC}m.txt \
            ${neo4j_warmup_neighbor_node} \
            ${neo4j_measure_neighbor_node} \
            ${pageCacheIgnoreIndexes}
      fi

    if [[ -n "$benchNeighbor" ]]; then
        sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        find /mnt2T/data/neo4j/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighbor neighbor-latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_neighbor_latency_jvm${JVM_HEAP}m_pagecache${PC}m.txt \
           ${neo4j_warmup_neighbor} \
           ${neo4j_measure_neighbor} \
           ${pageCacheIgnoreIndexes}
      fi

    if [[ -n "$benchNeighborAtype" ]]; then
        sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        find /mnt2T/data/neo4j/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighborAtype \
            latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
            ${HOME_DIR}/neo4j_${DATASET}_neighborAtype_latency_jvm${JVM_HEAP}m_pagecache${PC}m.txt \
            ${neo4j_warmup_neighbor_atype} \
            ${neo4j_measure_neighbor_atype} \
            ${pageCacheIgnoreIndexes}
      fi

      if [[ -n "$benchNeighborNodeIndexed" ]]; then
        sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        find /mnt2T/data/neo4j/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.NeighborNodeBench latency-index \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
            ${HOME_DIR}/neo4j_${DATASET}_neighbor_node_latency_index.txt \
            ${neo4j_warmup_neighbor_node} \
            ${neo4j_measure_neighbor_node} \
	    ${pageCacheIgnoreIndexes}
      fi

      if [[ -n "$benchMixed" ]]; then
        sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        find /mnt2T/data/neo4j/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
        find /mnt2T/data/neo4j/${DATASET}/schema/index -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;

        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.MixBench latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
           ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
           ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
           ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/node_query_${num_nodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_mix_neighbor_latency_jvm${JVM_HEAP}m_pagecache${pageCacheForNodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_mix_neighbor_node_latency_jvm${JVM_HEAP}m_pagecache${pageCacheForNodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_mix_neighborAtype_latency_jvm${JVM_HEAP}m_pagecache${pageCacheForNodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_mix_node_latency_jvm${JVM_HEAP}m_pagecache${pageCacheForNodes}.txt \
           ${HOME_DIR}/neo4j_${DATASET}_mix_node_node_latency_jvm${JVM_HEAP}m_pagecache${pageCacheForNodes}.txt \
           ${neo4j_warmup_mix} \
           ${neo4j_measure_mix} \
           ${pageCacheForNodes}

      fi

      for thputThreads in 30; do
      if [[ -n "$benchNhbrThput" ]]; then
        sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        find /mnt2T/data/neo4j/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;

        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighbor \
           neighbor-throughput \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
           ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
           NONE \
           0 \
           0 \
           ${thputThreads} \
           ${pageCacheForNodes}

        x=$(cut -d' ' -f1 neo4j_throughput_get_nhbrs.txt | awk '{ sum += $1 } END { print sum }')
        mv neo4j_throughput_get_nhbrs.txt neo4j_throughput_get_nhbrs-${thputThreads}clients.txt
        echo ${thputThreads} clients, $x get_nhbr queries/sec

      fi
    done

    if [[ -n "$benchAssocRange" ]]; then
        # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        find /mnt2T/data/neo4j/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
        cmd="java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocRange \
           latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/assocRange_warmup.txt \
           ${QUERY_DIR}/assocRange_query.txt \
           ${HOME_DIR}/neo4j_${DATASET}_assocRange_latency_jvm${JVM_HEAP}m_pagecache${pageCacheIgnoreIndexes}.txt \
           ${warmup_assocRange} \
           ${measure_assocRange} \
           ${pageCacheIgnoreIndexes}"
        echo $cmd
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocRange \
           latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/assocRange_warmup.txt \
           ${QUERY_DIR}/assocRange_query.txt \
           ${HOME_DIR}/neo4j_${DATASET}_assocRange_latency_jvm${JVM_HEAP}m_pagecache${pageCacheIgnoreIndexes}.txt \
           ${warmup_assocRange} \
           ${measure_assocRange} \
           ${pageCacheIgnoreIndexes}

      fi

    if [[ -n "$benchObjGet" ]]; then
        #sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        find /mnt2T/data/neo4j/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOObjGet \
           latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/objGet_warmup.txt \
           ${QUERY_DIR}/objGet_query.txt \
           ${HOME_DIR}/neo4j_${DATASET}_objGet_latency_jvm${JVM_HEAP}m_pagecache${pageCacheIgnoreIndexes}.txt \
           ${warmup_objGet} \
           ${measure_objGet} \
           ${pageCacheIgnoreIndexes}
      fi

    if [[ -n "$benchAssocGet" ]]; then
        #sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        find /mnt2T/data/neo4j/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocGet \
           latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/assocGet_warmup.txt \
           ${QUERY_DIR}/assocGet_query.txt \
           ${HOME_DIR}/neo4j_${DATASET}_assocGet_latency_jvm${JVM_HEAP}m_pagecache${pageCacheIgnoreIndexes}.txt \
           ${warmup_assocGet} \
           ${measure_assocGet} \
           ${pageCacheIgnoreIndexes}
      fi

      if [[ -n "$benchAssocCount" ]]; then
        #sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

        find /mnt2T/data/neo4j/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocCount \
           latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/assocCount_warmup.txt \
           ${QUERY_DIR}/assocCount_query.txt \
           ${HOME_DIR}/neo4j_${DATASET}_assocCount_latency_jvm${JVM_HEAP}m_pagecache${pageCacheIgnoreIndexes}.txt \
           ${warmup_assocCount} \
           ${measure_assocCount} \
           ${pageCacheIgnoreIndexes}
      fi

    if [[ -n "$benchAssocTimeRange" ]]; then
        #sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        find /mnt2T/data/neo4j/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
        java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
           edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocTimeRange \
           latency \
           ${NEO4J_DIR}/${DATASET} \
           ${QUERY_DIR}/assocTimeRange_warmup.txt \
           ${QUERY_DIR}/assocTimeRange_query.txt \
           ${HOME_DIR}/neo4j_${DATASET}_assocTimeRange_latency_jvm${JVM_HEAP}m_pagecache${pageCacheIgnoreIndexes}.txt \
           ${warmup_assocTimeRange} \
           ${measure_assocTimeRange} \
           ${pageCacheIgnoreIndexes}
    fi
      
    if [[ -n "$benchTAOMix" ]]; then
      sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

      java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
         edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocMixed \
         latency \
         ${NEO4J_DIR}/${DATASET} \
         ${QUERY_DIR}/assocRange_warmup.txt \
         ${QUERY_DIR}/assocRange_query.txt \
         ${QUERY_DIR}/assocCount_warmup.txt \
         ${QUERY_DIR}/assocCount_query.txt \
         ${QUERY_DIR}/objGet_warmup.txt \
         ${QUERY_DIR}/objGet_query.txt \
         ${QUERY_DIR}/assocGet_warmup.txt \
         ${QUERY_DIR}/assocGet_query.txt \
         ${QUERY_DIR}/assocTimeRange_warmup.txt \
         ${QUERY_DIR}/assocTimeRange_query.txt \
         ${HOME_DIR}/neo4j_${DATASET}_mix_assocRange_latency_jvm${JVM_HEAP}m_pagecache${pageCacheIgnoreIndexes}.txt \
         ${HOME_DIR}/neo4j_${DATASET}_mix_assocCount_latency_jvm${JVM_HEAP}m_pagecache${pageCacheIgnoreIndexes}.txt \
         ${HOME_DIR}/neo4j_${DATASET}_mix_objGet_latency_jvm${JVM_HEAP}m_pagecache${pageCacheIgnoreIndexes}.txt \
         ${HOME_DIR}/neo4j_${DATASET}_mix_assocGet_latency_jvm${JVM_HEAP}m_pagecache${pageCacheIgnoreIndexes}.txt \
         ${HOME_DIR}/neo4j_${DATASET}_mix_assocTimeRange_latency_jvm${JVM_HEAP}m_pagecache${pageCacheIgnoreIndexes}.txt \
         ${warmup_taoMix} \
         ${measure_taoMix} \
         ${pageCacheIgnoreIndexes}
    fi

    done
  done
done
