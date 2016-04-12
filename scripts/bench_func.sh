#!/bin/bash

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
source ${SCRIPT_DIR}/../conf/succinct-env.sh

NODE_FILE=${1:-/mnt2/twitter2010-40attr16each-tpch-npa${npa}sa${sa}isa${isa}.node}
EDGE_FILE=${2:-/mnt2/twitter2010-npa${npa}sa${sa}isa${isa}.assoc}
throughput_threads=${3:-""}
masterHostName=${4:-"localhost"}
copyQueries=${5:-"true"}
npa=${6:-"128"}
sa=${7:-"32"}
isa=${8:-"64"}
dataset=${9:-"twitter"}

echo "Node File: $NODE_FILE"
echo "Edge File: $EDGE_FILE"
echo "Query Path: $QUERY_DIR"
echo "Throughput Threads: $throughput_threads"
echo "Server: $masterHostName"

num_nodes=100000 # hack
augOpt="-augOpts"

# NOTE: comment this out for non-sharded bench
SHARDED=T
if [[ -z "$SHARDED" ]]; then
  TOTAL_NUM_SHARDS=no
fi

function bench() {
  echo "Benchmarking..."

  if [[ -n "$benchNode" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t node-latency -x ${warmup_node} \
      -y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/latency-node-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi

  if [[ -n "$benchNodeNode" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t node-node-latency -x ${warmup_node} \
      -y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/latency-nodeNode-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi

  if [[ -n "$benchNhbrNode" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t neighbor-node-latency -x ${warmup_neighbor_node} \
      -y ${measure_neighbor_node} -w ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/latency-neighborNode-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi

  if [[ -n "$benchNhbrAtype" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t neighbor-atype-latency -x ${warmup_neighbor_atype} \
      -y ${measure_neighbor_atype} -w ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
      -o ${HOME_DIR}/latency-neighborAtype-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi

  if [[ -n "$benchEdgeAttrs" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t edge-attrs-latency -x ${warmup_edgeAttrs} \
      -y ${measure_edgeAttrs} -w ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
      -o ${HOME_DIR}/latency-edgeAttrs-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi

  if [[ -n "$benchNhbr" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t neighbor-latency -x ${warmup_neighbor} \
      -y ${measure_neighbor} -w ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
      -o ${HOME_DIR}/latency-neighbor-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi

  if [[ -n "$benchPrimitiveMix" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t mix-latency \
      -x ${warmup_mix} -y ${measure_mix} \
      -w ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
      -a ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
      -b ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
      -c ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
      -d ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
      -e ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      -f ${QUERY_DIR}/node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/latency-mixNeighbor-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -h ${HOME_DIR}/latency-mixNeighborAtype-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -i ${HOME_DIR}/latency-mixNeighborNode-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -j ${HOME_DIR}/latency-mixNode-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -k ${HOME_DIR}/latency-mixNodeNode-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi
 
  if [[ -n "$benchTaoAssocRange" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t tao-assoc-range-latency \
      -x ${warmup_assocRange} -y ${measure_assocRange} \
      -w ${QUERY_DIR}/assocRange_warmup.txt -q ${QUERY_DIR}/assocRange_query.txt \
      -o ${HOME_DIR}/latency-taoAssocRange-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi

  if [[ -n "$benchTaoAssocCount" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t tao-assoc-count-latency \
      -x ${warmup_assocCount} -y ${measure_assocCount} \
      -w ${QUERY_DIR}/assocCount_warmup.txt -q ${QUERY_DIR}/assocCount_query.txt \
      -o ${HOME_DIR}/latency-taoAssocCount-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi

  if [[ -n "$benchTaoObjGet" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t tao-obj-get-latency \
      -x ${warmup_objGet} -y ${measure_objGet} \
      -w ${QUERY_DIR}/objGet_warmup.txt -q ${QUERY_DIR}/objGet_query.txt \
      -o ${HOME_DIR}/latency-taoObjGet-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi

  if [[ -n "$benchTaoAssocGet" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t tao-assoc-get-latency \
      -x ${warmup_assocGet} -y ${measure_assocGet} \
      -w ${QUERY_DIR}/assocGet_warmup.txt -q ${QUERY_DIR}/assocGet_query.txt \
      -o ${HOME_DIR}/latency-taoAssocGet-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi

  if [[ -n "$benchTaoAssocTimeRange" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t tao-assoc-time-range-latency \
      -x ${warmup_assocTimeRange} -y ${measure_assocTimeRange} \
      -w ${QUERY_DIR}/assocTimeRange_warmup.txt -q ${QUERY_DIR}/assocTimeRange_query.txt \
      -o ${HOME_DIR}/latency-taoAssocTimeRange-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi

  if [[ -n "$benchTaoAssocAdd" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t tao-assoc-add-latency \
      -o ${HOME_DIR}/latency-taoAssocAdd-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi
	
  if [[ -n "$benchTaoObjAdd" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t tao-obj-add-latency \
      -o ${HOME_DIR}/latency-taoObjAdd-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi

  if [[ -n "$benchTaoMix" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t tao-mix-latency \
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
      -o ${HOME_DIR}/latency-taoMixAssocRange-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -h ${HOME_DIR}/latency-taoMixAssocCount-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -i ${HOME_DIR}/latency-taoMixObjGet-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -j ${HOME_DIR}/latency-taoMixAssocGet-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -k ${HOME_DIR}/latency-taoMixAssocTimeRange-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi
	
  if [[ -n "$benchTaoMixWithUpdates" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t tao-mix-with-updates-latency \
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
      -o ${HOME_DIR}/latency-taoMixWithUpdatesAssocRange-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -h ${HOME_DIR}/latency-taoMixWithUpdatesAssocCount-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -i ${HOME_DIR}/latency-taoMixWithUpdatesObjGet-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -j ${HOME_DIR}/latency-taoMixWithUpdatesAssocGet-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -k ${HOME_DIR}/latency-taoMixWithUpdatesAssocTimeRange-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
	    -r ${HOME_DIR}/latency-taoMixWithUpdatesAssocAdd-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
	    -s ${HOME_DIR}/latency-taoMixWithUpdatesObjAdd-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
  fi
	
  if [[ -n "$benchNodeThput" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t node-throughput -x ${warmup_node} \
      -p ${throughput_threads} \
      -y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    o=throughput_get_nodes.txt
    x=$(cut -d' ' -f1 ${o} | awk '{sum += $1} END {print sum}')
    echo $throughput_threads clients, $x aggregated queries/sec >> ${o}
    mv ${o} \
      throughput_get_nodes-npa${npa}sa${sa}isa${isa}-${throughput_threads}clients.txt
  fi

  if [[ -n "$benchNodeNodeThput" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t node-node-throughput -x ${warmup_node} \
      -p ${throughput_threads} \
      -y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/double_node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}

    o=throughput_get_nodes2.txt
    x=$(cut -d' ' -f1 ${o} | awk '{sum += $1} END {print sum}')
    echo $throughput_threads clients, $x aggregated queries/sec >> ${o}
    mv ${o} \
      throughput_get_nodes2-npa${npa}sa${sa}isa${isa}-${throughput_threads}clients.txt
  fi

  if [[ -n "$benchNhbrNodeThput" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t neighbor-node-throughput -x ${warmup_neighbor_node} \
      -p ${throughput_threads} \
      -y ${measure_neighbor_node} -w ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/neighbor_node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}

    o=throughput_get_nhbrsNode.txt
    x=$(cut -d' ' -f1 ${o} | awk '{sum += $1} END {print sum}')
    echo $throughput_threads clients, $x aggregated queries/sec >> ${o}
    mv ${o} \
      throughput_get_nhbrsNode-npa${npa}sa${sa}isa${isa}-${throughput_threads}clients.txt
  fi

  if [[ -n "$benchNhbrAtypeThput" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t neighbor-atype-throughput -x ${warmup_neighbor_atype} \
      -p ${throughput_threads} \
      -y ${measure_neighbor_atype} -w ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
      -o ${HOME_DIR}/neighborAtype_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}

    o=throughput_get_nhbrsAtype.txt
    x=$(cut -d' ' -f1 ${o} | awk '{sum += $1} END {print sum}')
    echo $throughput_threads clients, $x aggregated queries/sec >> ${o}
    mv ${o} \
      throughput_get_nhbrsAtype-npa${npa}sa${sa}isa${isa}-${throughput_threads}clients.txt
  fi

  if [[ -n "$benchEdgeAttrsThput" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t edge-attrs-throughput -x ${warmup_edgeAttrs} \
      -p ${throughput_threads} \
      -y ${measure_edgeAttrs} -w ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}

    o=throughput_getEdgeAttrs.txt
    x=$(cut -d' ' -f1 ${o} | awk '{sum += $1} END {print sum}')
    echo $throughput_threads clients, $x aggregated queries/sec >> ${o}
    mv ${o} \
      throughput_getEdgeAttrs-npa${npa}sa${sa}isa${isa}-${throughput_threads}clients.txt
  fi

  if [[ -n "$benchNhbrThput" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t neighbor-throughput \
      -p ${throughput_threads} \
      -x ${warmup_neighbor} -y ${measure_neighbor} \
      -w ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}

    o=throughput_get_nhbrs.txt
    x=$(cut -d' ' -f1 ${o} | awk '{sum += $1} END {print sum}')
    echo $throughput_threads clients, $x aggregated queries/sec >> ${o}
    mv ${o} throughput_get_nhbrs-npa${npa}sa${sa}isa${isa}-${throughput_threads}clients.txt
  fi

  if [[ -n "$benchPrimitiveMixThput" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t mix-throughput \
      -p ${throughput_threads} \
      -x ${warmup_mix} -y ${measure_mix} \
      -w ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
      -a ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
      -b ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
      -c ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
      -d ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
      -e ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      -f ${QUERY_DIR}/node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/mix_neighbor_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -h ${HOME_DIR}/mix_neighborAtype_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -i ${HOME_DIR}/mix_neighbor_node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -j ${HOME_DIR}/mix_node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -k ${HOME_DIR}/mix_double_node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}

    o=throughput_mix.txt
    x=$(cut -d' ' -f1 ${o} | awk '{sum += $1} END {print sum}')
    echo $throughput_threads clients, $x aggregated queries/sec >> ${o}
    mv ${o} throughput_mix-npa${npa}sa${sa}isa${isa}-${throughput_threads}clients.txt
  fi
  
  if [[ -n "$benchTaoMixThput" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t tao-mix-throughput \
      -p ${throughput_threads} \
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
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}

    x=$(cut -d' ' -f1 throughput_tao_mix.txt | awk '{sum += $1} END {print sum}')
    echo $throughput_threads clients, $x aggregated queries/sec >> throughput_tao_mix.txt
    mv throughput_tao_mix.txt \
      throughput_tao_mix-npa${npa}sa${sa}isa${isa}-${throughput_threads}clients.txt
  fi

  if [[ -n "$benchTaoMixWithUpdatesThput" ]]; then
    ${BIN_DIR}/../benchmark/bin/bench -t tao-mix-with-updates-throughput \
      -p ${throughput_threads} \
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
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}

    x=$(cut -d' ' -f1 throughput_taoMixWithUpdates.txt | awk '{sum += $1} END {print sum}')
    echo $throughput_threads clients, $x aggregated queries/sec >> throughput_taoMixWithUpdates.txt
    mv throughput_taoMixWithUpdates.txt \
      throughput_taoMixWithUpdates-npa${npa}sa${sa}isa${isa}-${throughput_threads}clients.txt
  fi

  if [[ $# -eq 0 ]]; then
    if [[ -n "$SHARDED" ]]; then
    	bash ${SCRIPT_DIR}/../sbin/stop-all.sh
    fi
  fi
}

if [[ "$throughput_threads" == "" ]]; then
  for throughput_threads in 32 64; do
    sa=32; isa=64; npa=128; bench "$@"
  done
else
  sa=32; isa=64; npa=128; bench "$@" 2>&1
fi
