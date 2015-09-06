#!/bin/bash
set -e

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
source ${SCRIPT_DIR}/../conf/succinct-env.sh

num_nodes=100000 # hack

minDeg="-minDeg45"
minDeg="-minDeg60"
minDeg="-minDeg30"
minDeg=""
dataset="-liveJournal${minDeg}"
#dataset="-20attr35each"
#dataset="-40attr16each"
#dataset="-2attr350each"

minDegs=('-minDeg60')
minDegs=('' '-minDeg30')
minDegs=('-minDeg30')
minDegs=('-minDeg30WithTsAttr')
minDegs=('' '-minDeg30WithTsAttr')
minDegs=('')

# NOTE: comment this out for non-sharded bench
SHARDED=T
if [[ -z "$SHARDED" ]]; then
  TOTAL_NUM_SHARDS=no
fi

benchNeighbor=T
#benchNeighborAtype=T
#benchNeighborNode=T
#benchNode=T
#benchNodeNode=T
#benchMix=T

throughput_threads=4
#benchNeighborThput=T
#benchTaoMixThput=T

#benchAssocRange=T
#benchAssocCount=T
#benchObjGet=T
#benchAssocGet=T
#benchAssocTimeRange=T
#benchTaoMix=T

augOpt="-augOpts"

# hostname of the master aggregator that bench client connects to
# if desirable to put client on 1 host, and agg. on the other, change this
masterHostName="localhost"

function bench() {
  if [[ "$dataset" == "-liveJournal"* ]]; then
    pushd ${QUERY_DIR} >/dev/null
    yes | cp -rf liveJournal-40attr16each${minDeg}-queries/*txt ./
    popd >/dev/null
  else
    echo implement query copying for me! dataset: '${dataset}'
    exit 1
  fi

  #EDGE_FILE="data/higgs-social_network.opts-npa${npa}sa${sa}isa${isa}.edge_table"
  #NODE_FILE="data/higgs${dataset}-tpch-npa${npa}sa${sa}isa${isa}.nodeWithPtrs"
  EDGE_FILE="/mnt2T/data/liveJournal${augOpt}${minDeg}-npa${npa}sa${sa}isa${isa}.assoc"
  NODE_FILE="/mnt2T/data/liveJournal-40attr16each-tpch-npa${npa}sa${sa}isa${isa}.node"

  if [[ -n "$SHARDED" ]]; then
    bash ${SCRIPT_DIR}/../sbin/stop-all.sh
    sleep 2
  
    bash ${SCRIPT_DIR}/../sbin/start-servers.sh $NODE_FILE $EDGE_FILE $sa $isa $npa &
    sleep 2
  
    bash ${SCRIPT_DIR}/../sbin/start-handlers.sh &
    sleep 2
  fi

    if [[ -n "$benchNode" ]]; then
      sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t node-latency -x ${warmup_node} \
      -y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchNodeNode" ]]; then
      sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t node-node-latency -x ${warmup_node} \
      -y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/double_node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi
    
    if [[ -n "$benchNeighborNode" ]]; then
      sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t neighbor-node-latency -x ${warmup_neighbor_node} \
      -y ${measure_neighbor_node} -w ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
      -o ${HOME_DIR}/neighbor_node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchNeighborAtype" ]]; then
        sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        ${BIN_DIR}/bench -t neighbor-atype-latency -x ${warmup_neighbor_atype} \
        -y ${measure_neighbor_atype} -w ${QUERY_DIR}/neighborAtype_warmup_${num_nodes}.txt \
        -q ${QUERY_DIR}/neighborAtype_query_${num_nodes}.txt \
        -o ${HOME_DIR}/neighborAtype_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
        -m ${masterHostName} \
        ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchNeighbor" ]]; then
      sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t neighbor-latency -x ${warmup_neighbor} \
      -y ${measure_neighbor} -w ${QUERY_DIR}/neighbor_warmup_${num_nodes}.txt \
      -q ${QUERY_DIR}/neighbor_query_${num_nodes}.txt \
      -o ${HOME_DIR}/neighbor_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
      -m ${masterHostName} \
      ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchMix" ]]; then
      #sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

      ${BIN_DIR}/bench -t mix-latency \
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
    fi

    if [[ -n "$benchNeighborThput" ]]; then
      #sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

      ${BIN_DIR}/bench -t neighbor-throughput \
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

    if [[ -n "$benchAssocRange" ]]; then
      #sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t tao-assoc-range-latency \
        -x ${warmup_assocRange} -y ${measure_assocRange} \
        -w ${QUERY_DIR}/assocRange_warmup.txt -q ${QUERY_DIR}/assocRange_query.txt \
        -o ${HOME_DIR}/assocRange_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
        -m ${masterHostName} \
        ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchAssocCount" ]]; then
      #sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t tao-assoc-count-latency \
        -x ${warmup_assocCount} -y ${measure_assocCount} \
        -w ${QUERY_DIR}/assocCount_warmup.txt -q ${QUERY_DIR}/assocCount_query.txt \
        -o ${HOME_DIR}/assocCount_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
        -m ${masterHostName} \
        ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchObjGet" ]]; then
      #sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t tao-obj-get-latency \
        -x ${warmup_objGet} -y ${measure_objGet} \
        -w ${QUERY_DIR}/objGet_warmup.txt -q ${QUERY_DIR}/objGet_query.txt \
        -o ${HOME_DIR}/objGet_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
        -m ${masterHostName} \
        ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchAssocGet" ]]; then
      #sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t tao-assoc-get-latency \
        -x ${warmup_assocGet} -y ${measure_assocGet} \
        -w ${QUERY_DIR}/assocGet_warmup.txt -q ${QUERY_DIR}/assocGet_query.txt \
        -o ${HOME_DIR}/assocGet_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
        -m ${masterHostName} \
        ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchAssocTimeRange" ]]; then
      #sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ${BIN_DIR}/bench -t tao-assoc-time-range-latency \
        -x ${warmup_assocTimeRange} -y ${measure_assocTimeRange} \
        -w ${QUERY_DIR}/assocTimeRange_warmup.txt -q ${QUERY_DIR}/assocTimeRange_query.txt \
        -o ${HOME_DIR}/assocTimeRange_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
        -m ${masterHostName} \
        ${NODE_FILE} ${EDGE_FILE} ${SHARDED}
    fi

    if [[ -n "$benchTaoMix" ]]; then
      #sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

      ${BIN_DIR}/bench -t tao-mix-latency \
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
    fi

    if [[ -n "$benchTaoMixThput" ]]; then
      #sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

      ${BIN_DIR}/bench -t tao-mix-throughput \
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

    if [[ -n "$SHARDED" ]]; then
    	bash ${SCRIPT_DIR}/../sbin/stop-all.sh
    fi
}

#for throughput_threads in 32; do
#done
for minDeg in "${minDegs[@]}"; do
  dataset="-liveJournal${minDeg}"
  sa=32; isa=64; npa=128; bench
  #sa=8; isa=64; npa=64; bench
  #sa=4; isa=16; npa=16; bench
done
