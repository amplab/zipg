#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]}
do
    for freq in ${freqs[@]}
    do
        echo "Benching ${num_nodes}_${freq}"
        ${bin}/bench -t name-throughput -w ${data}/queries/warmup_${num_nodes}_${freq}.txt -q ${data}/queries/query_${num_nodes}_${freq}.txt -o ${dir}/name_throughput_results.txt ${data}/succinct/${num_nodes}_${freq}.graph
    done
done
