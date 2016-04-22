#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $0)
OUTPUT_DIR=${SCRIPT_DIR}/../results
mkdir -p $OUTPUT_DIR
source ${SCRIPT_DIR}/config.sh
classpath=${SCRIPT_DIR}/../target/scala-2.10/succinctgraph-assembly-0.1.0-SNAPSHOT.jar

TOTAL_MEM=249856 # r3.8xlarge
AVAIL_MEM=$(( $TOTAL_MEM - 512 )) # reserve 512m for OS

DATASET=orkut

NEO4J_DIR=/mnt/${DATASET}/neo4j
QUERY_DIR=/mnt/${DATASET}Queries

if [[ "$DATASET" == "twitter" ]]; then
  numNodes=41652230
  numAtypes=5
  minTime=1439721981221
  maxTime=1441905687237
elif [[ "$DATASET" == "orkut" ]]; then
  numNodes=3072627
  numAtypes=5
  minTime=1439721981221 # from twitter
  maxTime=1441905687237
else
  echo "Unrecognized dataset $DATASET."
  exit 1
fi

#### Latency
## Primitive Queries
#benchNeighbor=T
#benchNeighborAtype=T
#benchNeighborNode=T
#benchNode=T
#benchNodeNode=T
#benchNeighborNodeIndexed=T
#benchPrimitiveMix=T

## TAO Queries
#benchAssocRange=T
#benchObjGet=T
#benchAssocGet=T
#benchAssocCount=T
#benchAssocTimeRange=T
#benchTAOMix=T
#benchTAOWithUpdatesMix=T

#### Throughput
## Mixed workloads
#benchPrimtiveMixThput=T
#benchTaoMixThput=T
benchTaoMixWithUpdatesThput=T

## Primitive Queries
#benchNhbrThput=T
#benchNeighborAtypeThput=T
#benchEdgeAttrsThput=T
#benchNhbrNodeThput=T
#benchNodeNodeThput=T

## TAO Queries
#benchAssocRangeThput=T
#benchObjGetThput=T
#benchAssocGetThput=T
#benchAssocCountThput=T
#benchAssocTimeRangeThput=T

num_nodes=100000
PCPMIX="10g"
PCTMIX="10g"

for tuned in true false; do
  for thputThreads in 32; do
    #for PC in 1g 10g 50g 100g 150g 200g 220g 228g; do
      #mkdir -p res${PC}
      for JVM_HEAP in 15360; do
        # "use more": #`echo "0.75 * ($TOTAL_MEM - $JVM_HEAP)" | bc | awk '{printf("%d", $1)}'` #`echo "$AVAIL_MEM - $JVM_HEAP" | bc | awk '{printf("%d", $1)}'`
        # For default neo4j setting, use "Auto"; otherwise support custom value with postfix k/m/g
        echo "Setting -Xmx to ${JVM_HEAP}m"

        if [[ -n "$benchNodeNode" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/schema/index -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.BenchNode \
            node-node-latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/node_query_${num_nodes}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_node_node_latency_jvm${JVM_HEAP}m_pagecache${PC}m.txt \
            ${neo4j_warmup_node} \
            ${neo4j_measure_node} \
            ${thputThreads} \
            ${tuned} \
            ${PCPMIX}
        fi

        if [[ -n "$benchNodeNodeThput" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/schema/index -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.BenchNode \
            node-node-throughput \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/node_query_${num_nodes}.txt \
            DUMMY \
            ${neo4j_warmup_node} \
            ${neo4j_measure_node} \
            ${thputThreads} \
            ${tuned} \
            ${PCPMIX}

          o=neo4j_throughput_get_nodes2.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sum }')
          echo ${thputThreads} clients, $x aggregated queries/sec >> ${o}
          mv ${o} neo4j_throughput_get_nodes2-tuned_${tuned}-${thputThreads}clients.txt
        fi

        if [[ -n "$benchNeighborNode" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.NeighborNodeBench \
            latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_neighbor_node_latency_jvm${JVM_HEAP}m_pagecache${PC}m.txt \
            ${neo4j_warmup_neighbor_node} \
            ${neo4j_measure_neighbor_node} \
            ${thputThreads} \
            ${tuned} \
            ${PCPMIX}
        fi

        if [[ -n "$benchNhbrNodeThput" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.NeighborNodeBench \
            throughput \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
            DUMMY \
            ${neo4j_warmup_neighbor_node} \
            ${neo4j_measure_neighbor_node} \
            ${thputThreads} \
            ${tuned} \
            ${PCPMIX}

          o=neo4j_throughput_get_nhbrs_attr.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sum }')
          echo ${thputThreads} clients, $x aggregated queries/sec >> ${o}
          mv ${o} neo4j_throughput_get_nhbrs_attr-tuned_${tuned}-${thputThreads}clients.txt
        fi

        if [[ -n "$benchNeighbor" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighbor \
            neighbor-latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_neighbor_latency_jvm${JVM_HEAP}m_pagecache${PC}m.txt \
            ${neo4j_warmup_neighbor} \
            ${neo4j_measure_neighbor} \
            ${thputThreads} \
            ${tuned} \
            ${PCPMIX}
        fi

        if [[ -n "$benchNhbrThput" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighbor \
            neighbor-throughput \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
            DUMMY \
            ${neo4j_warmup_neighbor} \
            ${neo4j_measure_neighbor} \
            ${thputThreads} \
            ${tuned} \
            ${PCPMIX}

          o=neo4j_throughput_get_nhbrs.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sprintf("%.1f", sum) }')
          echo ${thputThreads} clients, $x get_nhbr queries/sec >> ${o}
          mv ${o} neo4j_throughput_get_nhbrs-tuned_${tuned}-${thputThreads}clients.txt
        fi

        if [[ -n "$benchNeighborAtype" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighborAtype \
            latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_neighborAtype_latency_jvm${JVM_HEAP}m_pagecache${PC}m.txt \
            ${neo4j_warmup_neighbor_atype} \
            ${neo4j_measure_neighbor_atype} \
            ${thputThreads} \
            ${tuned} \
            ${PCPMIX}
        fi

        if [[ -n "$benchNeighborAtypeThput" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighborAtype \
            throughput \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
            DUMMY \
            ${neo4j_warmup_neighbor_atype} \
            ${neo4j_measure_neighbor_atype} \
            ${thputThreads} \
            ${tuned} \
            ${PCPMIX}

          o=neo4j_throughput_get_nhbrs_atype.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sprintf("%.1f", sum) }')
          echo ${thputThreads} clients, $x aggregated queries/sec >> ${o}
          mv ${o} neo4j_throughput_get_nhbrs_atype-tuned_${tuned}-${thputThreads}clients.txt
        fi

        if [[ -n "$benchEdgeAttrs" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighborAtype \
            edgeAttrs-latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
            DUMMY \
            ${neo4j_warmup_edgeAttrs} \
            ${neo4j_measure_edgeAttrs} \
            ${thputThreads} \
            ${tuned} \
            ${PCPMIX}

          o=neo4j_throughput_getEdgeAttrs.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sprintf("%.1f", sum) }')
          echo ${thputThreads} clients, $x aggregated queries/sec >> ${o}
          mv ${o} neo4j_throughput_getEdgeAttrs-tuned_${tuned}-${thputThreads}clients.txt
        fi

        if [[ -n "$benchEdgeAttrsThput" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.BenchNeighborAtype \
            edgeAttrs-throughput \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
            DUMMY \
            ${neo4j_warmup_edgeAttrs} \
            ${neo4j_measure_edgeAttrs} \
            ${thputThreads} \
            ${tuned} \
            ${PCPMIX}

          o=neo4j_throughput_getEdgeAttrs.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sprintf("%.1f", sum) }')
          echo ${thputThreads} clients, $x aggregated queries/sec >> ${o}
          mv ${o} neo4j_throughput_getEdgeAttrs-tuned_${tuned}-${thputThreads}clients.txt
        fi

        if [[ -n "$benchPrimitiveMix" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          # find ${NEO4J_DIR}/${DATASET}/schema/index -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
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
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_neighbor_latency_jvm${JVM_HEAP}m_pagecache${PCPMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_neighbor_node_latency_jvm${JVM_HEAP}m_pagecache${PCPMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_neighborAtype_latency_jvm${JVM_HEAP}m_pagecache${PCPMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_node_latency_jvm${JVM_HEAP}m_pagecache${PCPMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_node_node_latency_jvm${JVM_HEAP}m_pagecache${PCPMIX}.txt \
            ${neo4j_warmup_mix} \
            ${neo4j_measure_mix} \
            ${thputThreads} \
            ${tuned} \
            ${PCPMIX}
        fi

        if [[ -n "$benchPrimtiveMixThput" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          # find ${NEO4J_DIR}/${DATASET}/schema/index -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.MixBench \
            throughput \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
            ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
            ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
            ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
            ${QUERY_DIR}/node_query_${num_nodes}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_neighbor_latency_jvm${JVM_HEAP}m_pagecache${PCPMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_neighbor_node_latency_jvm${JVM_HEAP}m_pagecache${PCPMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_neighborAtype_latency_jvm${JVM_HEAP}m_pagecache${PCPMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_node_latency_jvm${JVM_HEAP}m_pagecache${PCPMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_node_node_latency_jvm${JVM_HEAP}m_pagecache${PCPMIX}.txt \
            ${neo4j_warmup_mix} \
            ${neo4j_measure_mix} \
            ${thputThreads} \
            ${tuned} \
            ${PCPMIX}

          o=neo4j_throughput_mix.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sum }')
          echo ${thputThreads} clients, $x aggregated queries/sec >> ${o}
          mv ${o} neo4j_throughput_mix-tuned_${tuned}-${thputThreads}clients.txt
        fi

        if [[ -n "$benchAssocRange" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocRange \
            latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/assocRange_warmup.txt \
            ${QUERY_DIR}/assocRange_query.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_assocRange_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${neo4j_warmup_assocRange} \
            ${neo4j_measure_assocRange} \
            ${thputThreads} \
            ${tuned} \
            ${PCTMIX}
        fi

        if [[ -n "$benchAssocRangeThput" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocRange \
            throughput \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/assocRange_warmup.txt \
            ${QUERY_DIR}/assocRange_query.txt \
            DUMMY \
            ${neo4j_warmup_assocRange} \
            ${neo4j_measure_assocRange} \
            ${thputThreads} \
            ${tuned} \
            ${PCTMIX}

          o=neo4j_throughput_assoc_range.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sum }')
          echo ${thputThreads} clients, $x aggregated queries/sec >> ${o}
          mv ${o} neo4j_throughput_assoc_range-tuned_${tuned}-${thputThreads}clients.txt
        fi

        if [[ -n "$benchObjGet" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOObjGet \
            latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/objGet_warmup.txt \
            ${QUERY_DIR}/objGet_query.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_objGet_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${neo4j_warmup_objGet} \
            ${neo4j_measure_objGet} \
            ${thputThreads} \
            ${tuned} \
            ${PCTMIX}
        fi

        if [[ -n "$benchObjGetThput" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOObjGet \
            throughput \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/objGet_warmup.txt \
            ${QUERY_DIR}/objGet_query.txt \
            DUMMY \
            ${neo4j_warmup_objGet} \
            ${neo4j_measure_objGet} \
            ${thputThreads} \
            ${tuned} \
            ${PCTMIX}

          o=neo4j_throughput_obj_get.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sum }')
          echo ${thputThreads} clients, $x aggregated queries/sec >> ${o}
          mv ${o} neo4j_throughput_obj_get-tuned_${tuned}-${thputThreads}clients.txt
        fi

        if [[ -n "$benchAssocGet" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocGet \
            latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/assocGet_warmup.txt \
            ${QUERY_DIR}/assocGet_query.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_assocGet_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${neo4j_warmup_assocGet} \
            ${neo4j_measure_assocGet} \
            ${thputThreads} \
            ${tuned} \
            ${PCTMIX}
        fi

        if [[ -n "$benchAssocGetThput" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocGet \
            throughput \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/assocGet_warmup.txt \
            ${QUERY_DIR}/assocGet_query.txt \
            DUMMY \
            ${neo4j_warmup_assocGet} \
            ${neo4j_measure_assocGet} \
            ${thputThreads} \
            ${tuned} \
            ${PCTMIX}

          o=neo4j_throughput_assoc_get.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sum }')
          echo ${thputThreads} clients, $x aggregated queries/sec >> ${o}
          mv ${o} neo4j_throughput_assoc_get-tuned_${tuned}-${thputThreads}clients.txt
        fi

        if [[ -n "$benchAssocCount" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocCount \
            latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/assocCount_warmup.txt \
            ${QUERY_DIR}/assocCount_query.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_assocCount_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${neo4j_warmup_assocCount} \
            ${neo4j_measure_assocCount} \
            ${thputThreads} \
            ${tuned} \
            ${PCTMIX}
        fi

        if [[ -n "$benchAssocCountThput" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocCount \
            throughput \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/assocCount_warmup.txt \
            ${QUERY_DIR}/assocCount_query.txt \
            DUMMY \
            ${neo4j_warmup_assocCount} \
            ${neo4j_measure_assocCount} \
            ${thputThreads} \
            ${tuned} \
            ${PCTMIX}

          o=neo4j_throughput_assoc_count.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sum }')
          echo ${thputThreads} clients, $x aggregated queries/sec >> ${o}
          mv ${o} neo4j_throughput_assoc_count-tuned_${tuned}-${thputThreads}clients.txt
        fi

        if [[ -n "$benchAssocTimeRange" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocTimeRange \
            latency \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/assocTimeRange_warmup.txt \
            ${QUERY_DIR}/assocTimeRange_query.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_assocTimeRange_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${neo4j_warmup_assocTimeRange} \
            ${neo4j_measure_assocTimeRange} \
            ${thputThreads} \
            ${tuned} \
            ${PCTMIX}
        fi

        if [[ -n "$benchAssocTimeRangeThput" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOAssocTimeRange \
            throughput \
            ${NEO4J_DIR}/${DATASET} \
            ${QUERY_DIR}/assocTimeRange_warmup.txt \
            ${QUERY_DIR}/assocTimeRange_query.txt \
            DUMMY \
            ${neo4j_warmup_assocTimeRange} \
            ${neo4j_measure_assocTimeRange} \
            ${thputThreads} \
            ${tuned} \
            ${PCTMIX}

          o=neo4j_throughput_assoc_time_range.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sum }')
          echo ${thputThreads} clients, $x aggregated queries/sec >> ${o}
          mv ${o} neo4j_throughput_assoc_time_range-tuned_${tuned}-${thputThreads}clients.txt
        fi

        if [[ -n "$benchTAOMix" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOMixed \
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
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_assocRange_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_assocCount_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_objGet_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_assocGet_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_assocTimeRange_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${neo4j_warmup_taoMix} \
            ${neo4j_measure_taoMix} \
            ${thputThreads} \
            ${tuned} \
            ${numNodes} ${numAtypes} ${minTime} ${maxTime} \
            ${PCTMIX}
        fi

        if [[ -n "$benchTaoMixThput" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOMixed \
            throughput \
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
            DUMMY DUMMY DUMMY DUMMY DUMMY \
            ${neo4j_warmup_taoMix} \
            ${neo4j_measure_taoMix} \
            ${thputThreads} \
            ${tuned} \
            ${numNodes} ${numAtypes} ${minTime} ${maxTime} \
            ${PCTMIX}

          o=neo4j_throughput_tao_mix.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sum }')
          echo ${thputThreads} clients, $x aggregated queries/sec >> ${o}
          mv ${o} neo4j_throughput_tao_mix-tuned_${tuned}-${thputThreads}clients.txt
        fi

        if [[ -n "$benchTAOMixWithUpdates" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOMixedWithUpdates \
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
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_assocRange_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_assocCount_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_objGet_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_assocGet_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_assocTimeRange_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_assocAdd_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${OUTPUT_DIR}/neo4j_${DATASET}_mix_objGet_latency_jvm${JVM_HEAP}m_pagecache${PCTMIX}.txt \
            ${neo4j_warmup_taoMix} \
            ${neo4j_measure_taoMix} \
            ${thputThreads} \
            ${tuned} \
            ${numNodes} ${numAtypes} ${minTime} ${maxTime} \
            ${PCTMIX}
        fi

        if [[ -n "$benchTaoMixWithUpdatesThput" ]]; then
          # sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
          # find ${NEO4J_DIR}/${DATASET}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
          java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${JVM_HEAP}m -cp ${classpath} \
            edu.berkeley.cs.succinctgraph.neo4jbench.tao.BenchTAOMixedWithUpdates \
            throughput \
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
            DUMMY DUMMY DUMMY DUMMY DUMMY DUMMY DUMMY\
            ${neo4j_warmup_taoMix} \
            ${neo4j_measure_taoMix} \
            ${thputThreads} \
            ${tuned} \
            ${numNodes} ${numAtypes} ${minTime} ${maxTime} \
            ${PCTMIX}

          o=neo4j_throughput_tao_mix_with_updates.txt
          x=$(cut -d' ' -f1 ${o} | awk '{ sum += $1 } END { print sum }')
          echo ${thputThreads} clients, $x aggregated queries/sec >> ${o}
          mv ${o} neo4j_throughput_tao_mix_with_updates-tuned_${tuned}-${thputThreads}clients.txt
        fi
      done
      #mv neo4j_*.txt res${PC}
    #done
  done
done
