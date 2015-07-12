#!/bin/bash
set -e

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
source ${SCRIPT_DIR}/../conf/succinct-env.sh

num_nodes=100000 # hack
#dataset="-20attr35each"
dataset="-2attr350each"

# NOTE: comment this out for non-sharded bench
SHARDED=T

benchNeighbor=T
benchNeighborAtype=T
benchNeighborNode=T
benchNode=T
benchNodeNode=T

function bench() {
  EDGE_FILE="../data/higgs-social_network.opts-npa${npa}sa${sa}isa${isa}.assoc"
  NODE_FILE="../data/higgs${dataset}-tpch-npa${npa}sa${sa}isa${isa}.node"
  #NODE_FILE="${DATA_DIR}/higgs${dataset}-tpch-npa${npa}sa${sa}isa${isa}.nodeWithPtrs"

  bash ${SCRIPT_DIR}/../sbin/stop-all.sh
  sleep 2
  bash ${SCRIPT_DIR}/../sbin/start-servers.sh $NODE_FILE $EDGE_FILE &
  sleep 2
  bash ${SCRIPT_DIR}/../sbin/start-handlers.sh &
  sleep 2

    if [[ -n "$benchNode" ]]; then
      sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

      ${BIN_DIR}/bench -t node-latency -x ${warmup_node} \
      -y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}

    fi

    if [[ -n "$benchNodeNode" ]]; then
      sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t node-node-latency -x ${warmup_node} \
      -y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/double_node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi
    
    if [[ -n "$benchNeighborNode" ]]; then
      sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t neighbor-node-latency -x ${warmup_neighbor_node} \
      -y ${measure_neighbor_node} -w ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/neighbor_node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchNeighborAtype" ]]; then
        sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        ${BIN_DIR}/bench -t neighbor-atype-latency -x ${warmup_neighbor_atype} \
        -y ${measure_neighbor_atype} -w ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
        -q ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
        -o ${HOME_DIR}/neighborAtype_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
        ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchNeighbor" ]]; then
      sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t neighbor-latency -x ${warmup_neighbor} \
      -y ${measure_neighbor} -w ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
      -o ${HOME_DIR}/neighbor_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi


#  avg=$(cut -d',' -f2 ${HOME_DIR}/node_latency-npa${npa}sa${sa}isa${isa}.txt | awk '{x += $1} END {print x/NR}')
#  echo "npa${npa} sa${sa} isa${isa}, get_nodes(attr) average: ${avg}"

  bash ${SCRIPT_DIR}/../sbin/stop-all.sh
}


sa=32; isa=64; npa=128
bench

sa=8; isa=64; npa=64
bench

sa=4; isa=16; npa=16
bench
