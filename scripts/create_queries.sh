#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]} 
do
    echo creating queries for graph: ${num_nodes}
    ${bin}/create queries ${data}/nodes/${num_nodes}.node ${data}/queries/warmup_${num_nodes}.txt ${data}/queries/query_${num_nodes}.txt
done
