#!/bin/bash
source config.sh
neo4j_dir=${dir}/benchmark/neo4j-benchmark
javac -cp .:${neo4j_dir}/lib/neo4j-kernel-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-primitive-collections-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-io-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-lucene-index-2.2.0-RC01.jar:${neo4j_dir}/lib/lucene-core-3.6.2.jar ${neo4j_dir}/MixBench.java
for num_nodes in ${nodes[@]}
do
    java -XX:+UseConcMarkSweepGC -Xmx3000m -cp .:${neo4j_dir}:${neo4j_dir}/lib/neo4j-kernel-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-primitive-collections-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-io-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-lucene-index-2.2.0-RC01.jar:${neo4j_dir}/lib/lucene-core-3.6.2.jar MixBench ${data}/target/${num_nodes} ${data}/node-queries/warmup_${num_nodes}.txt ${data}/node-queries/query_${num_nodes}.txt ${data}/neighbor-queries/warmup_${num_nodes}.txt ${data}/neighbor-queries/query_${num_nodes}.txt ${warmup_n} ${measure_n} ${cooldown_n} ${dir}/neo4j_results.txt
done
