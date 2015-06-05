#!/bin/bash
set -e

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh

mkdir -p ${NEO4J_DIR}

for num_nodes in ${nodes[@]}
do
    if [ -d ${NEO4J_DIR}/${num_nodes} ]
    then
        echo "Warning: neo4j database ${NEO4J_DIR}/${num_nodes} exists, import will \
not force write; if you really want a new database, remove it first then \
re-run this script"
        exit
    fi

    echo "Creating neo4j-import csv for ${num_nodes} nodes"
    node_header=":ID"
    for (( i = 0; i < ${attributes}; i++ ))
    do
        node_header+=",name${i}"
    done
    echo $node_header > ${CSV_DIR}/nodes-header.csv

    EDGE_CSV=${CSV_DIR}/${num_nodes}_edge.csv
    # TODO: this sed is very slow, can just patch the OCaml program
    sed 's/\([0-9]*\) \([0-9]*\)/\1,\2,E/' ${EDGE_DIR}/${num_nodes}.edge > $EDGE_CSV &

    NODE_CSV=${CSV_DIR}/${num_nodes}_node.csv
    # input (line i): [attr]
    # output (line i): i-1, [attr]
    awk '{printf("%d,%s\n", NR-1, $0)}' ${NODE_DIR}/${num_nodes}.node > $NODE_CSV &
    wait

    sleep 5

    ${HOME_DIR}/external/neo4j/bin/neo4j-import --into ${NEO4J_DIR}/${num_nodes} \
        --nodes:Node "${CSV_DIR}/nodes-header.csv,${CSV_DIR}/${num_nodes}_node.csv" \
        --relationships "${CSV_DIR}/edges-header.csv,${CSV_DIR}/${num_nodes}_edge.csv" \
        --id-type INTEGER --stacktrace --bad-tolerance 0

    # create indexes on all attributes
    for (( i = 0; i < ${attributes}; i++ ))
    do
        ${HOME_DIR}/external/neo4j/bin/neo4j-shell -path ${NEO4J_DIR}/${num_nodes} \
            -c "CREATE INDEX ON :Node(name${i});"
    done
    ${HOME_DIR}/external/neo4j/bin/neo4j-shell -path ${NEO4J_DIR}/${num_nodes} \
        -c "schema await -l Node"
done
