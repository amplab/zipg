#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]}
do
    for freq in ${freqs[@]}
    do
        echo "Benching ${nodes}_${freq}"
        ./succinct-graph/bin/bench -t name-throughput -w queries/warmup_${nodes}_${freq}.txt -q queries/query_${nodes}_${freq}.txt -o name_throughput_results.txt ${nodes}_${freq}.graph
    done
done
