#!/bin/bash
for nodes in 1000 10000 100000 1000000
do
	freq=1
	while [ $freq -lt nodes ]
	do
		java -cp .:lib/neo4j-kernel-2.2.0-RC01.jar:lib/neo4j-primitive-collections-2.2.0-RC01.jar:lib/neo4j-io-2.2.0-RC01.jar:lib/neo4j-lucene-index-2.2.0-RC01.jar:lib/lucene-core-3.6.2.jar BenchName ${nodes}_${freq} 
		freq=$((freq * 10))
	done
done
