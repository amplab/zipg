#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]} 
do
    echo creating succinct graph, nodes: ${num_nodes} freq: ${freq}
    ${bin}/create succinct ${data}/nodes/${num_nodes}.node ${data}/edges/${num_nodes}.edge
    mv ${data}/nodes/${num_nodes}.graph ${data}/graphs
    mv ${data}/nodes/${num_nodes}.graph.succinct ${data}/succinct/
done
