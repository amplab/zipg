#!/bin/bash
SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
mkdir -p ${NODE_DIR}
for num_nodes in ${nodes[@]}
do
    echo Creating node file, num_nodes: ${num_nodes}, freq: ${freq}
    ${BIN_DIR}/create nodes ${num_nodes} ${attributes} ${freq} ${attribute_length}
    mv ${num_nodes}.node ${NODE_DIR}/${num_nodes}.node
done
