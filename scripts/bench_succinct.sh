#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]}
do
    echo "Benching ${num_nodes}, freq ${freq}"
    #${bin}/bench -t name-throughput -w ${data}/queries/warmup_${num_nodes}_${freq}.txt -q ${data}/queries/query_${num_nodes}_${freq}.txt -o ${dir}/name_throughput_results.txt ${data}/succinct/${num_nodes}_${freq}.graph
    ${bin}/bench -t mix-latency -x ${warmup_mix} -y ${measure_mix} -z ${cooldown_mix} -w ${data}/node-queries/warmup_${num_nodes}.txt -q ${data}/node-queries/query_${num_nodes}.txt -a ${data}/neighbor-queries/warmup_${num_nodes}.txt -b ${data}/neighbor-queries/query_${num_nodes}.txt -o ${dir}/${num_nodes}_mix_throughput_results.txt ${data}/succinct/${num_nodes}.graph
done
