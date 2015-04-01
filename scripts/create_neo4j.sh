#!/bin/bash
for nodes in 10000 100000 1000000 2000000
do
	sed 's/\([0-9]*\) \([0-9]*\)/\1,\2,E/' ${nodes}.edge > csv/${nodes}_edge.csv 
	freq=1
	while [ $freq -lt $nodes ]
	do
		awk '{printf("%d,%s\n", NR-1, $0)}' ${nodes}_${freq}.node > csv/${nodes}_${freq}_node.csv
		./neo4j/bin/neo4j-import --into ~/target/${nodes}_${freq} --nodes:Node csv/nodes-header.csv,csv/${nodes}_${freq}_node.csv --relationships csv/edges-header.csv,csv/${nodes}_edge.csv --id-type INTEGER
		./neo4j/bin/neo4j-shell -path ~/target/${nodes}_${freq} -c "CREATE INDEX ON :Node(name);" 
		freq=$((freq * 10))
	done
done
