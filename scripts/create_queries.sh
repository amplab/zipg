#!/bin/bash
SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
mkdir -p ${QUERY_DIR}

NODE_SUCCINCT_DIR=${DATA_DIR}/higgs.node.succinct
EDGE_SUCCINCT_DIR=${DATA_DIR}/higgs-activity_time.edge_table.succinct

for num_nodes in ${nodes[@]}
do
    echo creating neighbor queries for ${num_nodes} nodes, warmup ${warmup_neighbor}, measure ${measure_neighbor}

    ${BIN_DIR}/create neighbor-queries \
      ${num_nodes} \
      ${warmup_neighbor} \
      ${measure_neighbor} \
      ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
      ${QUERY_DIR}/neighbor_query_${num_nodes}.txt

    echo creating node queries for ${num_nodes} nodes, warmup ${warmup_node}, measure ${measure_node}

    ${BIN_DIR}/create node-queries \
      ${NODE_DIR}/${num_nodes}.node \
      ${warmup_node} \
      ${measure_node} \
      ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      ${QUERY_DIR}/node_query_${num_nodes}.txt

    echo creating neighbor-node queries for ${num_nodes} nodes, warmup ${warmup_neighbor_node}, measure ${measure_neighbor_node}

    ${BIN_DIR}/create neighbor-node-queries \
      ${NODE_SUCCINCT_DIR} \
      ${EDGE_SUCCINCT_DIR} \
      ${warmup_neighbor_node} \
      ${measure_neighbor_node} \
      ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
      ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt

done
