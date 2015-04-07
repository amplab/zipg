#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]} 
do
    for freq in ${freqs[@]}
    do
        echo creating queries for graph: ${num_nodes}_${freq}
        ${bin}/create queries ${data}/nodes/${num_nodes}_${freq}.node ${data}/queries/warmup_${num_nodes}_${freq}.txt ${data}/queries/query_${num_nodes}_${freq}.txt
    done
done
