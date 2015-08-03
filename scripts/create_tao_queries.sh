#!/bin/bash

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
mkdir -p ${QUERY_DIR}

assocRange=T
# assocGet=T
# assocCount=T
# assocTimeRange=T
# nodeGet=T

# SR doesn't matter since we are just loading, as long as the graph *has* been
# constructed.
npa=128
sa=32
isa=64
EDGE_FILE="../data/liveJournal-npa${npa}sa${sa}isa${isa}.assoc"
NODE_FILE="../data/liveJournal-40attr16each-tpch-npa${npa}sa${sa}isa${isa}.node"

function start_all() {
  stop_all
  sleep 2
  bash ${SCRIPT_DIR}/../sbin/start-servers.sh $NODE_FILE $EDGE_FILE &
  sleep 2
  bash ${SCRIPT_DIR}/../sbin/start-handlers.sh &
  sleep 2
}

function stop_all() {
  bash ${SCRIPT_DIR}/../sbin/stop-all.sh
}

if [[ -n "$assocRange" ]]; then
  start_all

  ${BIN_DIR}/create \
    tao-assoc-range-queries \
    $(wc -l ${NODE_FILE} | cut -d' ' -f 1) \
    ${max_num_atype} \
    ${warmup_assoc_range} \
    ${measure_assoc_range} \
    ${QUERY_DIR}/assocRange_warmup.txt \
    ${QUERY_DIR}/assocRange_query.txt

  stop_all
fi

