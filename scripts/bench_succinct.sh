#!/bin/bash
set -e

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh

for num_nodes in ${nodes[@]}
do
    echo "Benching ${num_nodes}"

    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    ${BIN_DIR}/bench -t neighbor-latency -x ${warmup_neighbor} \
		-y ${measure_neighbor} -w ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
		-q ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
		-o ${HOME_DIR}/${num_nodes}_neighbor_latency.txt \
		${NODE_FILE} ${EDGE_FILE}

    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    ${BIN_DIR}/bench -t node-latency -x ${warmup_node} \
		-y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
		-q ${QUERY_DIR}/node_query_${num_nodes}.txt \
		-o ${HOME_DIR}/${num_nodes}_node_latency.txt \
		${NODE_FILE} ${EDGE_FILE}

    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    ${BIN_DIR}/bench -t node-node-latency -x ${warmup_node} \
		-y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
		-q ${QUERY_DIR}/node_query_${num_nodes}.txt \
		-o ${HOME_DIR}/${num_nodes}_double_node_latency.txt \
		${NODE_FILE} ${EDGE_FILE}

    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    ${BIN_DIR}/bench -t neighbor-node-latency -x ${warmup_neighbor_node} \
		-y ${measure_neighbor_node} -w ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
		-q ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
		-o ${HOME_DIR}/${num_nodes}_neighbor_node_latency.txt \
		${NODE_FILE} ${EDGE_FILE} ${attribute_length} ${attributes}

    # ${BIN_DIR}/bench -t mix-latency -x ${warmup_mix} \
		# -y ${measure_mix} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
		# -q ${QUERY_DIR}/node_query_${num_nodes}.txt \
        # -a ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
        # -b ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
		# -o ${dir}/${num_nodes}_mix_latency.txt \
		# ${NODE_FILE} ${EDGE_FILE}

done
