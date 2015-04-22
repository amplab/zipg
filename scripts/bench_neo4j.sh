#!/bin/bash
source config.sh
neo4j_dir=${dir}/benchmark/neo4j
javac -cp .:${neo4j_dir}/lib/neo4j-kernel-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-primitive-collections-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-io-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-lucene-index-2.2.0-RC01.jar:${neo4j_dir}/lib/lucene-core-3.6.2.jar ${neo4j_dir}/*.java
for iter in 1 2 3
do
for num_nodes in ${nodes[@]}
do
    echo "Benching nodes: ${num_nodes}"
    sync
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    #java -XX:+UseConcMarkSweepGC -Xmx3000m -cp .:${neo4j_dir}:${neo4j_dir}/lib/neo4j-kernel-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-primitive-collections-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-io-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-lucene-index-2.2.0-RC01.jar:${neo4j_dir}/lib/lucene-core-3.6.2.jar MixBench latency ${data}/neo4j/${num_nodes} ${data}/node-queries/warmup_${num_nodes}.txt ${data}/node-queries/query_${num_nodes}.txt ${data}/neighbor-queries/warmup_${num_nodes}.txt ${data}/neighbor-queries/query_${num_nodes}.txt ${warmup_n} ${measure_n} ${cooldown_n} ${dir}/neo4j_results.txt
    java -XX:+UseConcMarkSweepGC -Xmx3000m -cp .:${neo4j_dir}:${neo4j_dir}/lib/neo4j-kernel-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-primitive-collections-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-io-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-lucene-index-2.2.0-RC01.jar:${neo4j_dir}/lib/lucene-core-3.6.2.jar BenchNode node-latency ${data}/neo4j/${num_nodes} ${data}/node-queries/warmup_${num_nodes}.txt ${data}/node-queries/query_${num_nodes}.txt ${dir}/${iter}_neo4j_${num_nodes}_node_latency.txt ${warmup_node} ${measure_node} ${cooldown_node}
    #java -XX:+UseConcMarkSweepGC -Xmx3000m -cp .:${neo4j_dir}:${neo4j_dir}/lib/neo4j-kernel-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-primitive-collections-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-io-2.2.0-RC01.jar:${neo4j_dir}/lib/neo4j-lucene-index-2.2.0-RC01.jar:${neo4j_dir}/lib/lucene-core-3.6.2.jar BenchNeighbor neighbor-latency ${data}/neo4j/${num_nodes} ${data}/neighbor-queries/warmup_${num_nodes}.txt ${data}/neighbor-queries/query_${num_nodes}.txt ${dir}/${iter}_neo4j_${num_nodes}_neighbor_latency.txt ${warmup_neighbor} ${measure_neighbor} ${cooldown_neighbor}
done
done
