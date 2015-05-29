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

nodes=( 100000 )
deg=20
freq=100
attributes=10
attribute_length=32
warmup_neighbor=500000
measure_neighbor=500000
warmup_node=50000
measure_node=100000
warmup_mix=100000
measure_mix=100000
