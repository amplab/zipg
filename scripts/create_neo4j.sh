#!/bin/bash
for nodes in 1000 10000 100000 1000000
do
	sed 's/\([0-9]*\) \([0-9]*\)/\1,\2,E/' ${nodes}.edge > ${nodes}_edge.csv 
	freq=1
	while [ $freq -le $nodes ]
	do
		../bin/create_nodes ${nodes} ${nodes} ${freq}
		awk '{printf("%d,%s\n", NR-1, $0)}' ${nodes}_${freq}.node > ${nodes}_${freq}_node.csv
		../../neo4j/bin/neo4j-import --into ~/target/${nodes}_${freq} --nodes:Nodes nodes-header.csv,${nodes}_${freq}_node.csv --relationships edges-header.csv,${nodes}_edge.csv --id-type INTEGER
		../../neo4j/bin/neo4j-shell -path ~/target/${nodes}_${freq} -c "CREATE INDEX ON :Node(name);" 
		freq=$((freq * 10))
	done
done
