#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]} 
do
    echo creating node queries for ${num_nodes} nodes, warmup ${warmup_node}, measure ${measure_node}
    ${bin}/create node-queries ${data}/nodes/${num_nodes}.node ${warmup_node} ${measure_node} ${data}/node-queries/warmup_${num_nodes}.txt ${data}/node-queries/query_${num_nodes}.txt
    echo creating neighbor queries for ${num_nodes} nodes, warmup ${warmup_neighbor}, measure ${measure_neighbor}
    ${bin}/create neighbor-queries ${num_nodes} ${warmup_neighbor} ${measure_neighbor} ${data}/neighbor-queries/warmup_${num_nodes}.txt ${data}/neighbor-queries/query_${num_nodes}.txt
done
