#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]} 
do
    for freq in ${freqs[@]} 
    do
        echo creating nodes: ${num_nodes}_${freq}
        ${bin}/create nodes ${num_nodes} ${attributes} ${freq}
        mv ${num_nodes}_${freq}.node ${data}/nodes
    done
done
