#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]} 
do
    for freq in ${freqs[@]}
    do
        echo creating succinct graph: ${num_nodes}_${freq}
        ${dir}/succinct-graph/bin/create succinct ${dir}/files/${num_nodes}_${freq}.node ${dir}/files/${num_nodes}.edge
    done
done
