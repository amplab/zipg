#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]} 
do
    echo creating queries for graph: ${num_nodes}
    ${bin}/create node-queries ${data}/nodes/${num_nodes}.node ${data}/node-queries/warmup_${num_nodes}.txt ${data}/node/queries/query_${num_nodes}.txt
    ${bin}/create neighbor-queries ${num_nodes} ${data}/neighbor-queries/warmup_${num_nodes}.txt ${data}/node/neighbor-queries/query_${num_nodes}.txt
done
