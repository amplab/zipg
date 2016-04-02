#!/bin/bash
set -e
num_nodes=$1
freq=$2

UPLOAD_DIR=${num_nodes}node-40deg-${freq}freq-10attr-32attrSize-256npa-64isa-64sa
mkdir -p $UPLOAD_DIR
pushd $UPLOAD_DIR

# data/csv
mkdir -p csv && pushd csv
ln -s ../../data/csv/${num_nodes}_edge.csv
ln -s ../../data/csv/${num_nodes}_node.csv
ln -s ../../data/csv/edges-header.csv
ln -s ../../data/csv/nodes-header.csv
popd

# data/edges
mkdir -p edges && pushd edges
ln -s ../../data/edges/${num_nodes}.edge
popd

# data/nodes
mkdir -p nodes && pushd nodes
ln -s ../../data/nodes/${num_nodes}.node
popd

# data/neo4j
mkdir -p neo4j && pushd neo4j
ln -s ../../data/neo4j/${num_nodes}
popd

# data/succinct
mkdir -p succinct && pushd succinct
ln -s ../../data/succinct/${num_nodes}.graph.succinct
popd

# data/queries
mkdir -p queries && pushd queries
ln -s ../../data/queries/*_${num_nodes}.txt ./
popd

popd
echo "All linking done: ${UPLOAD_DIR}."
