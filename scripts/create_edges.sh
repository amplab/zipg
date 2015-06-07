#!/usr/bin/env bash
set -e

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
mkdir -p ${EDGE_DIR}

for num_nodes in ${nodes[@]}
do
    echo "Creating ${EDGE_DIR}/${num_nodes}.edge"
    ${BIN_DIR}/generate_graphs ${num_nodes} $((deg * num_nodes / 2))
    mv ${num_nodes}.edge ${EDGE_DIR}
done
