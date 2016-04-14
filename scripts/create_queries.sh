#!/bin/bash
SCRIPT_DIR=$(dirname $0)
BIN_DIR=$SCRIPT_DIR/../benchmark/bin

query_dir=${1:-"DUMMY"}
num_nodes=${2:-"0"}
warmup_n=${3:-"20000"}
measure_n=${4:-"100000"}
NODE_FILE=${5:-"DUMMY"}
EDGE_FILE=${6:-"DUMMY"}

echo "query_dir=$query_dir"

num_node_attrs=40
attribute_length=16
max_num_atype=5

if [[ -n "$neighbor" ]]; then
  echo "creating neighbor queries for ${num_nodes} nodes, warmup ${warmup_n}, measure ${measure_n}"
  echo "numNode=$num_nodes"

  ${BIN_DIR}/create neighbor-queries \
    ${num_nodes} \
    ${warmup_n} \
    ${measure_n} \
    ${query_dir}/neighbor_warmup_100000.txt \
    ${query_dir}/neighbor_query_100000.txt 
fi

if [[ -n "$node" ]]; then
  echo "creating node queries for ${num_nodes} nodes, warmup ${warmup_n}, measure ${measure_n}"
  echo "numNodeAttrs=$num_node_attrs, attributes=$num_node_attrs, IS_NODE_FILE_CSV=0"

  ${BIN_DIR}/create node-queries \
    ${num_node_attrs} \
    ${warmup_n} \
    ${measure_n} \
    ${query_dir}/node_warmup_100000.txt \
    ${query_dir}/node_query_100000.txt \
    ${num_node_attrs} \
		0
fi

if [[ -n "$neighborNode" ]]; then
  echo "creating neighbor-node queries for ${num_nodes} nodes, warmup ${warmup_n}, measure ${measure_n}"
  echo "numNode=$num_nodes, attributes=$attributes"

  # load sharded graph to generate queries
  ${BIN_DIR}/create neighbor-node-queries \
   ${NODE_FILE} \
   ${EDGE_FILE} \
   ${num_nodes} \
   ${num_node_attrs} \
   ${warmup_n} \
   ${measure_n} \
   ${query_dir}/neighbor_node_warmup_100000.txt \
   ${query_dir}/neighbor_node_query_100000.txt 
fi

if [[ -n "$neighborAtype" ]]; then
  echo "creating neighbor-atype queries, warmup ${warmup_n}, measure ${measure_n}"
  echo "numNode=$num_nodes, max_num_atype=$max_num_atype"

  # queries can have empty results
  ${BIN_DIR}/create neighbor-atype-queries \
    ${num_nodes} \
    ${max_num_atype} \
    ${warmup_n} \
    ${measure_n} \
    ${query_dir}/neighborAtype_warmup_100000.txt \
    ${query_dir}/neighborAtype_query_100000.txt 
fi

## TAO Queries

if [[ -n "$assocRange" ]]; then
	
  echo "creating assocRange queries, warmup ${warmup_n}, measure ${measure_n}"
  echo "numNode=$num_nodes, max_num_atype=$max_num_atype"

  ${BIN_DIR}/create \
    tao-assoc-range-queries \
    ${num_nodes} \
    ${max_num_atype} \
    ${warmup_n} \
    ${measure_n} \
    ${query_dir}/assocRange_warmup.txt \
    ${query_dir}/assocRange_query.txt

fi

if [[ -n "$objGet" ]]; then
	
  echo "creating objGet queries, warmup ${warmup_n}, measure ${measure_n}"
  echo "numNode=$num_nodes"

  ${BIN_DIR}/create \
    tao-node-get-queries \
    ${num_nodes} \
    ${warmup_n} \
    ${measure_n} \
    ${query_dir}/objGet_warmup.txt \
    ${query_dir}/objGet_query.txt

fi

if [[ -n "$assocGet" ]]; then
	
  echo "creating assocGet queries, warmup ${warmup_n}, measure ${measure_n}"
  echo "numNode=$num_nodes, max_num_atype=$max_num_atype"

  ${BIN_DIR}/create \
    tao-assoc-get-queries \
    ${num_nodes} \
    ${max_num_atype} \
    ${warmup_n} \
    ${measure_n} \
    ${query_dir}/assocGet_warmup.txt \
    ${query_dir}/assocGet_query.txt

fi

if [[ -n "$assocCount" ]]; then
	
  echo "creating assocCount queries, warmup ${warmup_n}, measure ${measure_n}"
  echo "numNode=$num_nodes, max_num_atype=$max_num_atype"

  ${BIN_DIR}/create \
    tao-assoc-count-queries \
    ${num_nodes} \
    ${max_num_atype} \
    ${warmup_n} \
    ${measure_n} \
    ${query_dir}/assocCount_warmup.txt \
    ${query_dir}/assocCount_query.txt

fi

if [[ -n "$assocTimeRange" ]]; then
	
  echo "creating assocTimeRange queries, warmup ${warmup_n}, measure ${measure_n}"
  echo "numNode=$num_nodes, max_num_atype=$max_num_atype"

  ${BIN_DIR}/create \
    tao-assoc-time-range-queries \
    ${num_nodes} \
    ${max_num_atype} \
    ${warmup_n} \
    ${measure_n} \
    ${query_dir}/assocTimeRange_warmup.txt \
    ${query_dir}/assocTimeRange_query.txt

fi