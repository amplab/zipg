#!/bin/bash
neo4j_dir=${dir}/benchmark/neo4j-benchmark
${java_dir}/javac -cp .:${neo4j_dir}/lib/neo4j-kernel-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-primitive-collections-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-io-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-lucene-index-2.2.0-RC01.jar:${neo4j_dir}/lib/lucene-core-3.6.2.jar ${neo4j_dir}/BenchName.java
for num_nodes in ${nodes[@]}
do
	for freq in ${freqs[@]}
    do
        ${java_dir}/java -cp ./${neo4j_dir}:${neo4j_dir}/lib/neo4j-kernel-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-primitive-collections-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-io-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-lucene-index-2.2.0-RC01.jar:${neo4j_dir}/lib/lucene-core-3.6.2.jar BenchName ${data}/target/${nodes}_${freq} ${data}/queries/warmup_${nodes}_${freq}.txt ${data}/queries/query_${nodes}_${freq}.txt ${dir}/neo4j_results.txt
    done
done
