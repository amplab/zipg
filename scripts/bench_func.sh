#!/bin/bash
SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
source ${SCRIPT_DIR}/../conf/succinct-env.sh

dataset=${1:-"twitter"}
NODE_FILE="$HOME/data/$dataset/$dataset.node"
EDGE_FILE="$HOME/data/$dataset/$dataset.assoc"
throughput_threads=${2:-"128"}
masterHostName="localhost"
npa="128"
sa="32"
isa="64"

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

echo "Benchmarking..."

#${BIN_DIR}/../benchmark/bin/bench -t node-node-throughput -x ${warmup_node} \
#  -p ${throughput_threads} \
#  -y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
#  -q ${QUERY_DIR}/node_query_${num_nodes}.txt \
#  -o ${HOME_DIR}/double_node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
#  -m ${masterHostName} \
#  ${NODE_FILE} ${EDGE_FILE} ${SHARDED}

#o=throughput_get_nodes2.txt
#x=$(cut -d' ' -f1 ${o} | awk '{sum += $1} END {print sum}')
#echo $throughput_threads clients, $x aggregated queries/sec >> ${o}
#mv ${o} throughput_get_nodes2-npa${npa}sa${sa}isa${isa}-${throughput_threads}clients.txt

#${BIN_DIR}/../benchmark/bin/bench -t node-node2-throughput -x ${warmup_node} \
#  -p ${throughput_threads} \
#  -y ${measure_node} -w ${QUERY_DIR}/node_warmup_${num_nodes}.txt \
#  -q ${QUERY_DIR}/node_query_${num_nodes}.txt \
#  -o ${HOME_DIR}/double_node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
#  -m ${masterHostName} \
#  ${NODE_FILE} ${EDGE_FILE} ${SHARDED}

#o=throughput_get_nodes2.txt
#x=$(cut -d' ' -f1 ${o} | awk '{sum += $1} END {print sum}')
#echo $throughput_threads clients, $x aggregated queries/sec >> ${o}
#mv ${o} throughput_get_nodes22-npa${npa}sa${sa}isa${isa}-${throughput_threads}clients.txt

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
mv ${o} throughput_get_nhbrsNode-npa${npa}sa${sa}isa${isa}-${throughput_threads}clients.txt

${BIN_DIR}/../benchmark/bin/bench -t neighbor-node2-throughput -x ${warmup_neighbor_node} \
  -p ${throughput_threads} \
  -y ${measure_neighbor_node} -w ${QUERY_DIR}/neighbor_node_warmup_${num_nodes}.txt \
  -q ${QUERY_DIR}/neighbor_node_query_${num_nodes}.txt \
  -o ${HOME_DIR}/neighbor_node_latency-npa${npa}sa${sa}isa${isa}${dataset}-${TOTAL_NUM_SHARDS}shards.txt \
  -m ${masterHostName} \
  ${NODE_FILE} ${EDGE_FILE} ${SHARDED}

o=throughput_get_nhbrsNode.txt
x=$(cut -d' ' -f1 ${o} | awk '{sum += $1} END {print sum}')
echo $throughput_threads clients, $x aggregated queries/sec >> ${o}
mv ${o} throughput_get_nhbrsNode2-npa${npa}sa${sa}isa${isa}-${throughput_threads}clients.txt
