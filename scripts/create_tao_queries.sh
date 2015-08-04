#!/bin/bash

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
mkdir -p ${QUERY_DIR}

# assocRange=T
# assocGet=T
# assocCount=T
assocTimeRange=T
# objGet=T

# SR doesn't matter since we are just loading, as long as the graph *has* been
# constructed.
npa=128
sa=32
isa=64
EDGE_FILE="/mnt2T/data/liveJournal-npa${npa}sa${sa}isa${isa}.assoc"
NODE_FILE="/mnt2T/data/liveJournal-40attr16each-tpch-npa${npa}sa${sa}isa${isa}.node"

function stop_all() {
  bash ${SCRIPT_DIR}/../sbin/stop-all.sh
}

function start_all() {
  stop_all
  sleep 2
  bash ${SCRIPT_DIR}/../sbin/start-servers.sh $NODE_FILE $EDGE_FILE &
  sleep 2
  bash ${SCRIPT_DIR}/../sbin/start-handlers.sh &
  sleep 2
}

if [[ -n "$assocRange" ]]; then
  start_all

  ${BIN_DIR}/create \
    tao-assoc-range-queries \
    $(wc -l ${NODE_FILE} | cut -d' ' -f 1) \
    ${max_num_atype} \
    ${warmup_assocRange} \
    ${measure_assocRange} \
    ${QUERY_DIR}/assocRange_warmup.txt \
    ${QUERY_DIR}/assocRange_query.txt

  stop_all
fi

if [[ -n "$objGet" ]]; then

  ${BIN_DIR}/create \
    tao-node-get-queries \
    $(wc -l ${NODE_FILE} | cut -d' ' -f 1) \
    ${warmup_objGet} \
    ${measure_objGet} \
    ${QUERY_DIR}/objGet_warmup.txt \
    ${QUERY_DIR}/objGet_query.txt

fi

if [[ -n "$assocGet" ]]; then
  start_all

  ${BIN_DIR}/create \
    tao-assoc-get-queries \
    $(wc -l ${NODE_FILE} | cut -d' ' -f 1) \
    ${max_num_atype} \
    ${warmup_assocGet} \
    ${measure_assocGet} \
    ${ASSOC_FILE} \
    ${QUERY_DIR}/assocGet_warmup.txt \
    ${QUERY_DIR}/assocGet_query.txt

  stop_all
fi

if [[ -n "$assocCount" ]]; then
  ${BIN_DIR}/create \
    tao-assoc-count-queries \
    $(wc -l ${NODE_FILE} | cut -d' ' -f 1) \
    ${max_num_atype} \
    ${warmup_assocCount} \
    ${measure_assocCount} \
    ${QUERY_DIR}/assocCount_warmup.txt \
    ${QUERY_DIR}/assocCount_query.txt
fi

if [[ -n "$assocTimeRange" ]]; then
  start_all

  ${BIN_DIR}/create \
    tao-assoc-time-range-queries \
    $(wc -l ${NODE_FILE} | cut -d' ' -f 1) \
    ${max_num_atype} \
    ${warmup_assocTimeRange} \
    ${measure_assocTimeRange} \
    ${ASSOC_FILE} \
    ${QUERY_DIR}/assocTimeRange_warmup.txt \
    ${QUERY_DIR}/assocTimeRange_query.txt

  stop_all
fi
