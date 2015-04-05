#!/bin/bash
java_dir=/usr/lib/jvm/java-1.7.0-openjdk-amd64/bin
dir=succinct-graph/benchmark/neo4j-benchmark
${java_dir}/javac -cp .:${dir}/lib/neo4j-kernel-2.2.0-RC01.jar:${dir}/lib/neo4j-primitive-collections-2.2.0-RC01.jar:${dir}/lib/neo4j-io-2.2.0-RC01.jar:${dir}/lib/neo4j-lucene-index-2.2.0-RC01.jar:${dir}/lib/lucene-core-3.6.2.jar ${dir}/BenchName.java
for nodes in 50000000 #1000 10000 100000 1000000
do
    freq=10
    while [ $freq -lt 100 ]
    do
        ${java_dir}/java -cp ./${dir}:${dir}/lib/neo4j-kernel-2.2.0-RC01.jar:${dir}/lib/neo4j-primitive-collections-2.2.0-RC01.jar:${dir}/lib/neo4j-io-2.2.0-RC01.jar:${dir}/lib/neo4j-lucene-index-2.2.0-RC01.jar:${dir}/lib/lucene-core-3.6.2.jar BenchName ${nodes} ${freq}
        freq=$((freq * 10))
    done
done
