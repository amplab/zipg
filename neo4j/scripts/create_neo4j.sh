#!/bin/bash
set -e

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
#### At least dependent on these configs (config.sh):
####    attribtues
####    NODE_FILE
####    ASSOC_FILE
####    NEO4J_DELIM

#DATASET="higgs-twitter-20attr35each"
DATASET="higgs-twitter-40attr16each"

NEO4J_DIR=/mnt3/neo4j
DATASET="liveJournal-40attr16each-minDeg30"
DATASET="liveJournal-40attr16each-minDeg45"
DATASET="liveJournal-40attr16each-minDeg60"
DATASET="liveJournal-40attr16each-minDeg30WithTsAttr"
DATASET="twitter2010-40attr16each"

mkdir -p ${NEO4J_DIR}
if [ -d ${NEO4J_DIR}/${DATASET} ]
then
    echo "Warning: neo4j database ${NEO4J_DIR}/${DATASET} exists, import will \
not force write; if you really want a new database, remove it first then \
re-run this script"
    exit
fi

#echo "Creating neo4j-import csv for ${NODE_FILE} and ${ASSOC_FILE}"
#
node_header=":ID"
for (( i = 0; i < ${attributes}; i++ ))
do
    node_header+="${NEO4J_DELIM}name${i}"
done
echo $node_header > ${CSV_DIR}/nodes-header.csv
#
## NOTE: encode atype as neo4j's primitive :TYPE (string);
##       also, timestamp is marked as LONG, so supposedly better than strings
echo \
    ":START_ID${NEO4J_DELIM}:END_ID${NEO4J_DELIM}:TYPE${NEO4J_DELIM}timestamp:LONG${NEO4J_DELIM}attr" \
    >${CSV_DIR}/edges-header.csv

${BENCH_BIN_DIR}/create neo4j-node $NODE_FILE ${NODE_FILE}.neo4j &
${BENCH_BIN_DIR}/create neo4j-edge $ASSOC_FILE ${ASSOC_FILE}.neo4j &
wait

#    EDGE_CSV=${CSV_DIR}/${num_nodes}_edge.csv
#    # TODO: this sed is very slow, can just patch the OCaml program
#    sed 's/\([0-9]*\) \([0-9]*\)/\1,\2,E/' ${EDGE_DIR}/${num_nodes}.edge > $EDGE_CSV &
#
#    NODE_CSV=${CSV_DIR}/${num_nodes}_node.csv
#    # input (line i): [attr]
#    # output (line i): i-1, [attr]
#    awk '{printf("%d,%s\n", NR-1, $0)}' ${NODE_DIR}/${num_nodes}.node > $NODE_CSV &
#    wait

sleep 5

# NOTE: --id-type ACTUAL supposedly will handle ID mismatch issues
${HOME_DIR}/external/neo4j/bin/neo4j-import \
    --into ${NEO4J_DIR}/${DATASET} \
    --delimiter ${NEO4J_DELIM} \
    --nodes:Node "${CSV_DIR}/nodes-header.csv,${NODE_FILE}.neo4j" \
    --relationships "${CSV_DIR}/edges-header.csv,${ASSOC_FILE}.neo4j" \
    --id-type ACTUAL \
    --stacktrace --bad-tolerance 0

# create indexes on all attributes
for (( i = 0; i < ${attributes}; i++ ))
do
    ${HOME_DIR}/external/neo4j/bin/neo4j-shell -path ${NEO4J_DIR}/${DATASET} \
        -c "CREATE INDEX ON :Node(name${i});"
done
${HOME_DIR}/external/neo4j/bin/neo4j-shell -path ${NEO4J_DIR}/${DATASET} \
    -c "schema await -l Node"
