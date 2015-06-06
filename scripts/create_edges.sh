#!/usr/bin/env bash
SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
mkdir -p ${EDGE_DIR}

# NOTE: `deg` is not really degree: it's really [actual avg outgoing degree / 2]. 
# Just view this as saying we need deg*num_nodes undirected edges (and the 
# `generate_graphs` program will output twice that many, directed.
for num_nodes in ${nodes[@]}
do
    echo "Creating ${EDGE_DIR}/${num_nodes}.edge"
    ${BIN_DIR}/generate_graphs ${num_nodes} $((deg * num_nodes))
    mv ${num_nodes}.edge ${EDGE_DIR}
done
