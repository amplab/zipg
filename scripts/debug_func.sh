#!/bin/bash

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
source ${SCRIPT_DIR}/../conf/succinct-env.sh

npa=128; sa=32; isa=64

NODE_FILE=${1:-/mnt2/twitter2010-40attr16each-tpch.node}
EDGE_FILE=${2:-/mnt2/twitter2010-npa128sa32isa64.assoc}

# hostname of the master aggregator that bench client connects to
# if desirable to put client on 1 host, and agg. on the other, change this
masterHostName=${3:-"localhost"}

echo "Node File: $NODE_FILE"
echo "Edge File: $EDGE_FILE"
echo "Query Path: $QUERY_DIR"
echo "Master Host Name: $masterHostName"

num_nodes=100000 # hack
dataset="twitter2010-40attr16each"

function debug() {
    ${BIN_DIR}/../benchmark/bin/bench -t debug \
      -x ${warmup_taoMix} -y ${measure_taoMix} \
      -w ${QUERY_DIR}/assocCount_warmup.txt \
      -q ${QUERY_DIR}/assocCount_query.txt \
      -a ${QUERY_DIR}/assocRange_warmup.txt \
      -b ${QUERY_DIR}/assocRange_query.txt \
      -c ${QUERY_DIR}/objGet_warmup.txt \
      -d ${QUERY_DIR}/objGet_query.txt \
      -e ${QUERY_DIR}/assocGet_warmup.txt \
      -f ${QUERY_DIR}/assocGet_query.txt \
      -g ${QUERY_DIR}/assocTimeRange_warmup.txt \
      -l ${QUERY_DIR}/assocTimeRange_query.txt \
      -o ${HOME_DIR}/mix_assocRange_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -h ${HOME_DIR}/mix_assocCount_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -i ${HOME_DIR}/mix_objGet_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -j ${HOME_DIR}/mix_assocGet_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -k ${HOME_DIR}/mix_assocTimeRange_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
}

debug "$@"
