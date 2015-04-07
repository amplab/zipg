#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]} 
do
    for freq in ${freqs[@]}
    do
        echo creating succinct graph: ${num_nodes}_${freq}
        ${bin}/create succinct ${data}/nodes/${num_nodes}_${freq}.node ${data}/edges/${num_nodes}.edge
        mv ${data}/nodes/${num_nodes}_${freq}.graph ${data}/graphs
        mv ${data}/nodes/${num_nodes}_${freq}.graph.succinct ${data}/succinct
    done
done
