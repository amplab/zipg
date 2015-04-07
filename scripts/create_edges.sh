#!/usr/bin/env bash
source config.sh
for num_nodes in ${nodes[@]} 
do
    echo "Creating edge/${num_nodes}.edge"
    ${dir}/../graphs/generate_graphs ${num_nodes}
    mv ${num_nodes}.edge ${data}/edges
done
