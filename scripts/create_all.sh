#!/bin/bash
set -e
SCRIPT_DIR=$(dirname $0)

bash ${SCRIPT_DIR}/create_edges.sh &
bash ${SCRIPT_DIR}/create_nodes.sh &
wait

echo "edges, nodes, queries done"
bash ${SCRIPT_DIR}/create_neo4j.sh &
bash ${SCRIPT_DIR}/create_succinct.sh && bash ${SCRIPT_DIR}/create_queries.sh
wait
echo "all done"
