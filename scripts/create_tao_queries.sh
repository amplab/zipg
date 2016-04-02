#!/bin/bash

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
mkdir -p ${QUERY_DIR}

assocRange=T
assocGet=T
assocCount=T
assocTimeRange=T
objGet=T

numNode=41652230

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
