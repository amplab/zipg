#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]} 
do
    echo creating nodes: ${num_nodes}_${freq}
    ${bin}/create nodes ${num_nodes} ${attributes} ${freq} ${attribute_length}
    mv ${num_nodes}.node ${data}/nodes/${num_nodes}.node
done
