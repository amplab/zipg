#!/bin/bash
source config.sh
num_nodes=10000000
for graph in ${nodes[@]}
do
    echo "Benching ${num_nodes}"
    #${bin}/bench -t neighbor-latency -x ${warmup_neighbor} -y ${measure_neighbor} -z ${cooldown_neighbor} -w ${data}/neighbor-queries/warmup_${num_nodes}.txt -q ${data}/neighbor-queries/query_${num_nodes}.txt -o ${dir}/${graph}_neighbor_latency_results.txt ${data}/succinct/${graph}.graph
    ${bin}/bench -t node-latency -x ${warmup_node} -y ${measure_node} -z ${cooldown_node} -w ${data}/node-queries/warmup_${num_nodes}.txt -q ${data}/node-queries/query_${num_nodes}.txt -o ${dir}/${graph}_name_latency_results.txt ${data}/succinct/${graph}.graph
    #${bin}/bench -t mix-latency -x ${warmup_mix} -y ${measure_mix} -z ${cooldown_mix} -w ${data}/node-queries/warmup_${num_nodes}.txt -q ${data}/node-queries/query_${num_nodes}.txt -a ${data}/neighbor-queries/warmup_${num_nodes}.txt -b ${data}/neighbor-queries/query_${num_nodes}.txt -o ${dir}/${graph}_mix_latency_results.txt ${data}/succinct/${graph}.graph
done
