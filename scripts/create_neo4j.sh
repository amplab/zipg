#!/bin/bash
source config.sh
for num_nodes in ${nodes[@]}
do
    echo "Creating csv for ${num_nodes} nodes"
    sed 's/\([0-9]*\) \([0-9]*\)/\1,\2,E/' ${data}/edges/${num_nodes}.edge > ${data}/csv/${num_nodes}_edge.csv 
    awk '{printf("%d,%s\n", NR-1, $0)}' ${data}/nodes/${num_nodes}.node > ${data}/csv/${num_nodes}_node.csv
    ${dir}/external/neo4j/bin/neo4j-import --into ${data}/neo4j/${num_nodes} --nodes:Node ${data}/csv/nodes-header.csv,${data}/csv/${num_nodes}_node.csv --relationships ${data}/csv/edges-header.csv,${data}/csv/${num_nodes}_edge.csv --id-type INTEGER
    for (( i = 0; i < ${attributes}; i++ ))
    do
        ${dir}/external/neo4j/bin/neo4j-shell -path ${data}/neo4j/${num_nodes} -c "CREATE INDEX ON :Node(name${i});" 
    done
done
