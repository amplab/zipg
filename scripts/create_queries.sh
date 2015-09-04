#!/bin/bash
SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
mkdir -p ${QUERY_DIR}

#neighborAtype=T
#node=T
#neighbor=T
neighborNode=T

for num_nodes in ${nodes[@]}
do

  if [[ -n "$neighbor" ]]; then
    echo creating neighbor queries for ${num_nodes} nodes, warmup ${warmup_neighbor}, measure ${measure_neighbor}

    ${BIN_DIR}/create neighbor-queries \
      $(wc -l ${NODE_FILE} | cut -d' ' -f 1) \
      ${warmup_neighbor} \
      ${measure_neighbor} \
      ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
      ${QUERY_DIR}/neighbor_query_${num_nodes}.txt
  fi

  if [[ -n "$node" ]]; then
   echo creating node queries for ${num_nodes} nodes, warmup ${warmup_node}, measure ${measure_node}

    ${BIN_DIR}/create node-queries \
      ${NODE_FILE} \
      ${warmup_node} \
      ${measure_node} \
      ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      ${QUERY_DIR}/node_query_${num_nodes}.txt \
      ${attributes} \
      ${IS_NODE_FILE_CSV}
  fi

  if [[ -n "$neighborNode" ]]; then
    echo creating neighbor-node queries for ${num_nodes} nodes, warmup ${warmup_neighbor_node}, measure ${measure_neighbor_node}

    # load sharded graph to generate queries
   ${BIN_DIR}/create neighbor-node-queries \
     ${NODE_FILE} \
     ${EDGE_FILE} \
     $(wc -l ${NODE_FILE} | cut -d' ' -f 1) \
     ${attributes} \
     ${warmup_neighbor_node} \
     ${measure_neighbor_node} \
     ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
     ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt

    # if noLoad, queries can have empty results
    # ${BIN_DIR}/create neighbor-node-queries-noLoad \
      # ${NODE_FILE} \
      # ${warmup_neighbor_node} \
      # ${measure_neighbor_node} \
      # ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
      # ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
      # ${attributes} \
      # ${IS_NODE_FILE_CSV}

 fi

  if [[ -n "$neighborAtype" ]]; then
    echo creating neighbor-atype queries, warmup ${warmup_neighbor_node}, measure ${measure_neighbor_node}

    # queries can have empty results
    ${BIN_DIR}/create neighbor-atype-queries \
      $(wc -l ${NODE_FILE} | cut -d' ' -f 1) \
      ${max_num_atype} \
      ${warmup_neighbor_atype} \
      ${measure_neighbor_atype} \
      ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
      ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt
  fi

done
