#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]} 
do
	for freq in ${freqs[@]} 
	do
		echo creating nodes: ${num_nodes}_${freq}
		${dir}/succinct-graph/bin/create nodes ${num_nodes} ${freq}
	done
done
