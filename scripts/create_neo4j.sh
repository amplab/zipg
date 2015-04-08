#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]}
do
	echo "Creating csv for ${num_nodes} nodes"
	sed 's/\([0-9]*\) \([0-9]*\)/\1,\2,E/' ${data}/edges/${num_nodes}.edge > ${data}/csv/${num_nodes}_edge.csv 
	for freq in ${freqs[@]}
	do
		awk '{printf("%d,%s\n", NR-1, $0)}' ${data}/nodes/${num_nodes}_${freq}.node > ${data}/csv/${num_nodes}_${freq}_node.csv
		${dir}/external/neo4j/bin/neo4j-import --into ${data}/target/${num_nodes}_${freq} --nodes:Node ${data}/csv/nodes-header.csv,${data}/csv/${num_nodes}_${freq}_node.csv --relationships ${data}/csv/edges-header.csv,${data}/csv/${num_nodes}_edge.csv --id-type INTEGER
		${dir}/external/neo4j/bin/neo4j-shell -path ${data}/target/${num_nodes}_${freq} -c "CREATE INDEX ON :Node(name);" 
	done
done
