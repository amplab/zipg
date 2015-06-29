#!/bin/bash
SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
mkdir -p ${QUERY_DIR}

for num_nodes in ${nodes[@]}
do
    echo creating neighbor queries for ${num_nodes} nodes, warmup ${warmup_neighbor}, measure ${measure_neighbor}

    ${BIN_DIR}/create neighbor-queries \
      ${NODE_FILE} \
      ${EDGE_FILE} \
      ${warmup_neighbor} \
      ${measure_neighbor} \
      ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
      ${QUERY_DIR}/neighbor_query_${num_nodes}.txt

    echo creating node queries for ${num_nodes} nodes, warmup ${warmup_node}, measure ${measure_node}

    ${BIN_DIR}/create node-queries \
      ${NODE_FILE} \
      ${warmup_node} \
      ${measure_node} \
      ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      ${QUERY_DIR}/node_query_${num_nodes}.txt \
      IS_NODE_FILE_CSV

    echo creating neighbor-node queries for ${num_nodes} nodes, warmup ${warmup_neighbor_node}, measure ${measure_neighbor_node}

    ${BIN_DIR}/create neighbor-node-queries \
      ${NODE_FILE} \
      ${EDGE_FILE} \
      ${attribute_length} \
      ${attributes} \
      ${warmup_neighbor_node} \
      ${measure_neighbor_node} \
      ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
      ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt

    echo creating neighbor-atype queries, warmup ${warmup_neighbor_node}, measure ${measure_neighbor_node}

    ${BIN_DIR}/create neighbor-atype-queries \
      ${NODE_FILE} \
      ${EDGE_FILE} \
      ${max_num_atype} \
      ${warmup_neighbor_atype} \
      ${measure_neighbor_atype} \
      ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
      ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt

done
