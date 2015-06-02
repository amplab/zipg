#!/bin/bash
SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
num_nodes=100000
for graph in ${nodes[@]}
do
    echo "Benching ${graph}"

    ${BIN_DIR}/bench -t neighbor-latency -x ${warmup_neighbor} \
		-y ${measure_neighbor} -w ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
		-q ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
		-o ${HOME_DIR}/${graph}_neighbor_latency.txt ${SUCCINCT_DIR}/${graph}.graph

    ${BIN_DIR}/bench -t node-latency -x ${warmup_node} \
		-y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
		-q ${QUERY_DIR}/node_query_${num_nodes}.txt \
		-o ${HOME_DIR}/${graph}_node_latency.txt ${SUCCINCT_DIR}/${graph}.graph

    ${BIN_DIR}/bench -t mix-latency -x ${warmup_mix} \
		-y ${measure_mix} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
		-q ${QUERY_DIR}/node_query_${num_nodes}.txt \
        -a ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
        -b ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
		-o ${dir}/${graph}_mix_latency.txt ${SUCCINCT_DIR}/${graph}.graph

    ${BIN_DIR}/bench -t node-node-latency -x ${warmup_node} \
		-y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
		-q ${QUERY_DIR}/node_query_${num_nodes}.txt \
		-o ${HOME_DIR}/${graph}_double_node_latency.txt ${SUCCINCT_DIR}/${graph}.graph

    ${BIN_DIR}/bench -t neighbor-node-latency -x ${warmup_neighbor_node} \
		-y ${measure_neighbor_node} -w ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
		-q ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
		-o ${HOME_DIR}/${graph}_neighbor_node_latency.txt ${SUCCINCT_DIR}/${graph}.graph
done
