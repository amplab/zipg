#!/bin/bash
for nodes in 10000 100000 1000000 2000000 3000000 4000000 5000000 
do
	freq=1
	while [ $freq -lt $nodes ]
	do
		echo ${nodes}_${freq}
		./succinct-graph/bin/bench -t name-throughput -w queries/warmup_${nodes}_${freq}.txt -q queries/query_${nodes}_${freq}.txt -o name_throughput_results.txt ${nodes}_${freq}.node ${nodes}.edge ${nodes}_${freq}.graph
		freq=$((freq * 10))
	done
done
