#!/bin/bash
SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
mkdir -p ${QUERY_DIR}

neighborAtype=T
node=T
neighbor=T
neighborNode=T

#numNode=$(wc -l ${NODE_FILE} | cut -d' ' -f 1) # calculate once
numNode=41652230
numNodeAttrs=40

NODE_FILE=/mnt2/twitter2010-40attr16each-tpch-npa128sa32isa64.node
EDGE_FILE=/mnt2/twitter2010-npa128sa32isa64.edge_table

function stop_all() {
  bash ${SCRIPT_DIR}/../sbin/stop-all.sh
}

function start_all() {
  stop_all
  sleep 2
  bash ${SCRIPT_DIR}/../sbin/start-servers.sh $NODE_FILE $EDGE_FILE &
  sleep 2
  bash ${SCRIPT_DIR}/../sbin/start-handlers.sh &
  sleep 2
  bash ${SCRIPT_DIR}/../sbin/load-data.sh &
  sleep 2
}

for num_nodes in ${nodes[@]}
do
  if [[ -n "$neighbor" ]]; then
    echo "creating neighbor queries for ${num_nodes} nodes, warmup ${warmup_neighbor}, measure ${measure_neighbor}"
    echo "numNode=$numNode"

    ${BIN_DIR}/../benchmark/bin/create neighbor-queries \
      ${numNode} \
      ${warmup_neighbor} \
      ${measure_neighbor} \
      ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
      ${QUERY_DIR}/neighbor_query_${num_nodes}.txt 
  fi

  if [[ -n "$node" ]]; then
    echo "creating node queries for ${num_nodes} nodes, warmup ${warmup_node}, measure ${measure_node}"
    echo "numNodeAttrs=$numNodeAttrs, attributes=$attributes, IS_NODE_FILE_CSV=$IS_NODE_FILE_CSV"

    ${BIN_DIR}/../benchmark/bin/create node-queries \
      ${numNodeAttrs} \
      ${warmup_node} \
      ${measure_node} \
      ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      ${QUERY_DIR}/node_query_${num_nodes}.txt \
      ${attributes} \
      ${IS_NODE_FILE_CSV} 
  fi

  if [[ -n "$neighborNode" ]]; then
    echo "creating neighbor-node queries for ${num_nodes} nodes, warmup ${warmup_neighbor_node}, measure ${measure_neighbor_node}"
    echo "numNode=$numNode, attributes=$attributes"

    # load sharded graph to generate queries
    ${BIN_DIR}/../benchmark/bin/create neighbor-node-queries \
     ${NODE_FILE} \
     ${EDGE_FILE} \
     ${numNode} \
     ${attributes} \
     ${warmup_neighbor_node} \
     ${measure_neighbor_node} \
     ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
     ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt 
  fi

  if [[ -n "$neighborAtype" ]]; then
    echo "creating neighbor-atype queries, warmup ${warmup_neighbor_atype}, measure ${measure_neighbor_atype}"
    echo "numNode=$numNode, max_num_atype=$max_num_atype"

    # queries can have empty results
    ${BIN_DIR}/../benchmark/bin/create neighbor-atype-queries \
      ${numNode} \
      ${max_num_atype} \
      ${warmup_neighbor_atype} \
      ${measure_neighbor_atype} \
      ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
      ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt 
  fi
done
