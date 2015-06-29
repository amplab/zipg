#!/usr/bin/env bash
SCRIPT_DIR=$(dirname $0)
HOME_DIR=${SCRIPT_DIR}/..

BIN_DIR=${HOME_DIR}/bin
DATA_DIR=${HOME_DIR}/data

NODE_DIR=${DATA_DIR}/nodes
EDGE_DIR=${DATA_DIR}/edges
GRAPH_DIR=${DATA_DIR}/graphs
SUCCINCT_DIR=${DATA_DIR}/succinct
CSV_DIR=${DATA_DIR}/csv
NEO4J_DIR=${DATA_DIR}/neo4j
QUERY_DIR=${DATA_DIR}/queries

NODE_FILE=${DATA_DIR}/higgs-2attr350each-tpch.node
EDGE_FILE=${DATA_DIR}/higgs-social_network.edge_table
IS_NODE_FILE_CSV=0
ASSOC_FILE=${DATA_DIR}/higgs-social_network.assoc # used for formatting neo4j
# \x02
NEO4J_DELIM=

# graph construction configs
nodes=( 100000 ) # list of num_nodes to bench
deg=40 # average outgoing degree (so, total 2 * (# of directed edges) = deg * num_nodes)
freq=100
attributes=2
attribute_length=350
max_num_atype=5

# succinct construction configs
sa_sampling_rate=64
isa_sampling_rate=64
npa_sampling_rate=256

# benchmark configs
JVM_HEAP=16g

warmup_neighbor=0
warmup_neighbor=100000
measure_neighbor=2000
measure_neighbor=200000
neo4j_warmup_neighbor=20000 # 5k is not sufficient
neo4j_measure_neighbor=100000

warmup_node=1000
warmup_node=50000
measure_node=2000
measure_node=100000
neo4j_warmup_node=20000 # 5k is not sufficient
neo4j_measure_node=100000

warmup_neighbor_node=10
warmup_neighbor_node=100000
measure_neighbor_node=2000
measure_neighbor_node=200000
neo4j_warmup_neighbor_node=100000
neo4j_measure_neighbor_node=200000

warmup_neighbor_atype=100000
measure_neighbor_atype=200000
neo4j_warmup_neighbor_atype=100000
neo4j_measure_neighbor_atype=200000

warmup_mix=50000
measure_mix=100000
neo4j_warmup_mix=100000
neo4j_measure_mix=200000
