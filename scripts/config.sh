#!/usr/bin/env bash
SCRIPT_DIR=$(dirname $0)
HOME_DIR=${SCRIPT_DIR}/..

BIN_DIR=${HOME_DIR}/bin
BENCH_BIN_DIR=${HOME_DIR}/benchmark/bin
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


# npa 64, sa/isa 16
NODE_FILE=${DATA_DIR}/higgs-2attr350each-tpch-npa64saIsa16.node
EDGE_FILE=${DATA_DIR}/higgs-social_network.lessPad.noPadHeaderWidths-npa64saIsa16.edge_table

# npa 128, sa/isa 32
NODE_FILE=${DATA_DIR}/higgs-2attr350each-tpch-npa128saIsa32.node
EDGE_FILE=${DATA_DIR}/higgs-social_network.lessPad.noPadHeader-npa128saIsa32.edge_table # also, no pad 2 widths

# npa 16, sa/isa4
NODE_FILE=${DATA_DIR}/higgs-2attr350each-tpch-npa16saIsa4.node
EDGE_FILE=${DATA_DIR}/higgs-social_network.lessPad.noPadHeaderWidths-npa16saIsa4.edge_table

# npa 32, sa/isa8
NODE_FILE=${DATA_DIR}/higgs-2attr350each-tpch-npa32saIsa8.node
EDGE_FILE=${DATA_DIR}/higgs-social_network.lessPad.noPadHeaderWidths-npa32saIsa8.edge_table

# level 0
NODE_FILE=${DATA_DIR}/higgs-2attr350each-tpch-npa128sa32isa64.nodeWithPtrs
EDGE_FILE=${DATA_DIR}/higgs-social_network.opts-npa128sa32isa64.edge_table

# 20attr, 35 each
NODE_FILE=${DATA_DIR}/higgs-20attr35each-tpch.node
EDGE_FILE=${DATA_DIR}/higgs-social_network.opts-npa32sa2isa32.edge_table

# 40attr, 16 each, higgs
NODE_FILE=${DATA_DIR}/higgs-40attr16each-tpch.node

# live journal
NODE_FILE=/mnt2T/data/liveJournal-40attr16each-tpch.node

# source assoc file, used for neo4j creation 
ASSOC_FILE=/mnt2T/data/liveJournal-minDeg30.assoc
ASSOC_FILE=/mnt2T/data/liveJournal-minDeg45.assoc
ASSOC_FILE=/mnt2T/data/liveJournal-minDeg60.assoc
ASSOC_FILE=/mnt2T/data/liveJournal.assoc 
ASSOC_FILE=/mnt2T/data/liveJournal-minDeg30WithTsAttr.assoc

# twitter-2010, for neo4j / query creation
NODE_FILE=/mnt2T/twitter2010-40attr16each-tpch.node
ASSOC_FILE=/mnt2T/twitter/twitter2010-npa128sa32isa64.assoc

# \x02
NEO4J_DELIM=
IS_NODE_FILE_CSV=0

# graph construction configs
nodes=( 100000 ) # list of num_nodes to bench
deg=40 # average outgoing degree (so, total 2 * (# of directed edges) = deg * num_nodes)

freq=100
attributes=40
attribute_length=16
max_num_atype=5

# succinct construction configs
sa_sampling_rate=64
isa_sampling_rate=64
npa_sampling_rate=256

# benchmark configs
JVM_HEAP=16g

warmup_neighbor=0
warmup_neighbor=200000
measure_neighbor=200
measure_neighbor=400000
neo4j_warmup_neighbor=100000 # 5k is not sufficient
neo4j_measure_neighbor=200000

warmup_node=0
warmup_node=200000
measure_node=2000
measure_node=400000
neo4j_warmup_node=50000 # 5k is not sufficient
neo4j_measure_node=100000

warmup_neighbor_node=100
warmup_neighbor_node=200000
measure_neighbor_node=2000
measure_neighbor_node=400000
neo4j_warmup_neighbor_node=100000
neo4j_measure_neighbor_node=200000

warmup_neighbor_atype=0
warmup_neighbor_atype=200000
measure_neighbor_atype=2000
measure_neighbor_atype=400000
neo4j_warmup_neighbor_atype=100000
neo4j_measure_neighbor_atype=200000

warmup_mix=400000
measure_mix=800000
neo4j_warmup_mix=100000
neo4j_measure_mix=400000

## TAO queries
warmup_assocRange=0
warmup_assocRange=200000
measure_assocRange=5
measure_assocRange=400000

warmup_objGet=200000
measure_objGet=400000

warmup_assocGet=200000
measure_assocGet=400000

warmup_assocCount=0
warmup_assocCount=200000
measure_assocCount=5
measure_assocCount=400000

warmup_assocTimeRange=200000
measure_assocTimeRange=400000

warmup_taoMix=400000
measure_taoMix=800000
