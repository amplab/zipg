#!/bin/bash
set -e

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh

# NOTE: comment this out for non-sharded bench
SHARDED=T

benchNeighbor=T
benchNeighborAtype=T
#benchNeighborNode=T
#benchNode=T
#benchNodeNode=T
benchMix=T

for num_nodes in ${nodes[@]}
do
    echo "Benching ${num_nodes}"

    if [[ -n "$benchNeighbor" ]]; then
      sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t neighbor-latency -x ${warmup_neighbor} \
      -y ${measure_neighbor} -w ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
      -o ${HOME_DIR}/${num_nodes}_neighbor_latency.txt \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchNeighborNode" ]]; then
      sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t neighbor-node-latency -x ${warmup_neighbor_node} \
      -y ${measure_neighbor_node} -w ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/${num_nodes}_neighbor_node_latency.txt \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchNode" ]]; then
      sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t node-latency -x ${warmup_node} \
      -y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/${num_nodes}_node_latency.txt \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchNodeNode" ]]; then
      sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t node-node-latency -x ${warmup_node} \
      -y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/${num_nodes}_double_node_latency.txt \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchNeighborAtype" ]]; then
      sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t neighbor-atype-latency -x ${warmup_neighbor_atype} \
      -y ${measure_neighbor_atype} -w ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
      -o ${HOME_DIR}/neighborAtype_latency.txt \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchMix" ]]; then
      sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

      ${BIN_DIR}/bench -t mix-latency \
        -x ${warmup_mix} -y ${measure_mix} \
        -w ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
        -q ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
        -a ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
        -b ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
        -c ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
        -d ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
        -e ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
        -f ${QUERY_DIR}/node_query_${num_nodes}.txt \
        -o ${HOME_DIR}/mix_neighbor_latency.txt \
        -h ${HOME_DIR}/mix_neighborAtype_latency.txt \
        -i ${HOME_DIR}/mix_neighbor_node_latency.txt \
        -j ${HOME_DIR}/mix_node_latency.txt \
        -k ${HOME_DIR}/mix_double_node_latency.txt

    fi
done
