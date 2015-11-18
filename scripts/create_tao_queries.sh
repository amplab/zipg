#!/bin/bash

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
mkdir -p ${QUERY_DIR}

#assocRange=T
assocGet=T
assocCount=T
assocTimeRange=T
#objGet=T

# SR doesn't matter since we are just loading, as long as the graph *has* been
# constructed.
npa=128
sa=32
isa=64
minDeg=""
minDeg="-minDeg60"
minDeg="-minDeg30WithTsAttr"
EDGE_FILE="/mnt2T/data/liveJournal${minDeg}-npa${npa}sa${sa}isa${isa}.assoc"
NODE_FILE="/mnt2T/data/liveJournal-40attr16each-tpch-npa${npa}sa${sa}isa${isa}.node"
ASSOC_FILE=/mnt2T/data/liveJournal-minDeg30WithTsAttr.assoc

#numNode=$(wc -l ${NODE_FILE} | cut -d' ' -f 1) # calculate once, before the next line...
numNode=41652230
NODE_FILE=/mnt/twitter2010-40attr16each-tpch-npa128sa32isa64.node
EDGE_FILE=/mnt2T/twitter2010-npa128sa32isa64.assoc
ASSOC_FILE=/mnt2T/twitter2010.assoc

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
  bash ${SCRIPT_DIR}/../sbin/load-data.sh &
  sleep 2
}

stop_all
start_all

if [[ -n "$assocRange" ]]; then

  ${BIN_DIR}/../benchmark/bin/create \
    tao-assoc-range-queries \
    ${numNode} \
    ${max_num_atype} \
    ${warmup_assocRange} \
    ${measure_assocRange} \
    ${QUERY_DIR}/assocRange_warmup.txt \
    ${QUERY_DIR}/assocRange_query.txt

fi

if [[ -n "$objGet" ]]; then

  ${BIN_DIR}/../benchmark/bin/create \
    tao-node-get-queries \
    ${numNode} \
    ${warmup_objGet} \
    ${measure_objGet} \
    ${QUERY_DIR}/objGet_warmup.txt \
    ${QUERY_DIR}/objGet_query.txt

fi

if [[ -n "$assocGet" ]]; then

  ${BIN_DIR}/../benchmark/bin/create \
    tao-assoc-get-queries \
    ${numNode} \
    ${max_num_atype} \
    ${warmup_assocGet} \
    ${measure_assocGet} \
    ${ASSOC_FILE} \
    ${QUERY_DIR}/assocGet_warmup.txt \
    ${QUERY_DIR}/assocGet_query.txt

fi

if [[ -n "$assocCount" ]]; then

  ${BIN_DIR}/../benchmark/bin/create \
    tao-assoc-count-queries \
    ${numNode} \
    ${max_num_atype} \
    ${warmup_assocCount} \
    ${measure_assocCount} \
    ${QUERY_DIR}/assocCount_warmup.txt \
    ${QUERY_DIR}/assocCount_query.txt

fi

if [[ -n "$assocTimeRange" ]]; then

  ${BIN_DIR}/../benchmark/bin/create \
    tao-assoc-time-range-queries \
    ${numNode} \
    ${max_num_atype} \
    ${warmup_assocTimeRange} \
    ${measure_assocTimeRange} \
    ${ASSOC_FILE} \
    ${QUERY_DIR}/assocTimeRange_warmup.txt \
    ${QUERY_DIR}/assocTimeRange_query.txt

fi

wait
stop_all
