#!/bin/bash
SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh

mkdir -p ${GRAPH_DIR}
mkdir -p ${SUCCINCT_DIR}

for num_nodes in ${nodes[@]}
do
    echo creating succinct graph, nodes: ${num_nodes} freq: ${freq}
    ${BIN_DIR}/create graph ${NODE_DIR}/${num_nodes}.node ${EDGE_DIR}/${num_nodes}.edge

    echo encoding generated graph using succinct, output to ${SUCCINCT_DIR}/${num_nodes}.graph.succinct
    ${BIN_DIR}/create succinct ${NODE_DIR}/${num_nodes}.graph

    mv ${NODE_DIR}/${num_nodes}.graph ${GRAPH_DIR}
    mv ${NODE_DIR}/${num_nodes}.graph.succinct ${SUCCINCT_DIR}
done
