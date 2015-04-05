#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]} 
do
	echo "Creating csv for ${num_nodes} nodes"
	sed 's/\([0-9]*\) \([0-9]*\)/\1,\2,E/' ${dir}/files/${num_nodes}.edge > ${dir}/csv/${num_nodes}_edge.csv 
	for freq in ${freqs[@]} 
	do
		awk '{printf("%d,%s\n", NR-1, $0)}' ${dir}/files/${num_nodes}_${freq}.node > ${dir}/csv/${num_nodes}_${freq}_node.csv
		${dir}/neo4j/bin/neo4j-import --into ${dir}/target/${num_nodes}_${freq} --nodes:Node ${dir}/csv/nodes-header.csv,${dir}/csv/${num_nodes}_${freq}_node.csv --relationships ${dir}/csv/edges-header.csv,${dir}/csv/${num_nodes}_edge.csv --id-type INTEGER
		${dir}/neo4j/bin/neo4j-shell -path ${dir}/target/${num_nodes}_${freq} -c "CREATE INDEX ON :Node(name);" 
	done
done
