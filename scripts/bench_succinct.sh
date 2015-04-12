#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]}
do
    echo "Benching ${num_nodes}, freq ${freq}"
    #${bin}/bench -t name-throughput -w ${data}/queries/warmup_${num_nodes}_${freq}.txt -q ${data}/queries/query_${num_nodes}_${freq}.txt -o ${dir}/name_throughput_results.txt ${data}/succinct/${num_nodes}_${freq}.graph
    ${bin}/bench -t mix-throughput -x ${warmup_n} -y ${measure_n} -z ${cooldown_n} -w ${data}/node-queries/warmup_${num_nodes}.txt -q ${data}/node-queries/query_${num_nodes}.txt -a ${data}/neighbor-queries/warmup_${num_nodes}.txt -b ${data}/neighbor-queries/query_${num_nodes}.txt -o ${dir}/name_throughput_results.txt ${data}/succinct/${num_nodes}.graph
done
