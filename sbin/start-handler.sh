# Usage: start-handler.sh <#shards> <handler#>
usage="Usage: start-handler.sh <# local shards> <handler#>"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/succinct-config.sh"

. "$SUCCINCT_PREFIX/sbin/load-succinct-env.sh"

bin="$SUCCINCT_HOME/bin"
bin="`cd "$bin"; pwd`"

export LD_LIBRARY_PATH=$SUCCINCT_HOME/lib

if [ "$SUCCINCT_DATA_PATH" = "" ]; then
  SUCCINCT_DATA_PATH="$SUCCINCT_HOME/dat"
fi

if [ "$SUCCINCT_LOG_PATH" = "" ]; then
	SUCCINCT_LOG_PATH="$SUCCINCT_HOME/log"
fi

if [ "$TOTAL_NUM_SHARDS" = "" ]; then
	TOTAL_NUM_SHARDS=1
fi

# number of shards to launch on each physical node
num_shards_local=$1
# which id identifies this physical node?
local_host_id=$2

# optionally support input file override
# default is to read from sbin/succinct-config.sh
node_file_raw=$3
if [ "$node_file_raw" = "" ]; then
  node_file_raw=${NODE_FILE}
fi
edge_file_raw=$4
if [ "$edge_file_raw" = "" ]; then
  edge_file_raw=${EDGE_FILE}
fi

# These can be set when calling this script; otherwise, use defaults
# Only need be set / meaningful when constructing graphs (if loading, no effects)
sa_sr=${5:-32}
isa_sr=${6:-64}
npa_sr=${7:-128}

if [ "$num_shards_local" = "" ]; then
  num_shards_local=1
fi

if [ "$local_host_id" = "" ]; then
  local_host_id=1
fi

if [ "$num_hosts" = "" ]; then
  num_hosts=1
fi

if [ "$SUCCINCT_LOG_PATH" = "" ]; then
	SUCCINCT_LOG_PATH="$SUCCINCT_HOME/log"
fi

storeMode=succinctstore
num_succinctstore_hosts=$num_hosts
if [[ "$ENABLE_MULTI_STORE" == T ]]; then
  if [[ $(( num_hosts - 1 )) == $local_host_id ]]; then
    storeMode=logstore
  fi
  num_succinctstore_hosts=$(( num_hosts ))
fi

mkdir -p $SUCCINCT_LOG_PATH

nohup "${bin}/../rpc/bin/graph_query_aggregator" \
  -t $TOTAL_NUM_SHARDS \
  -s $num_shards_local \
  -i $local_host_id \
  -h "${SUCCINCT_CONF_DIR}/hosts" \
  -m "${ENABLE_MULTI_STORE}" \
  -f "${NUM_SUFFIXSTORE_PARTS}" \
  -l "${NUM_LOGSTORE_PARTS}" \
  -x ${sa_sr} -y ${isa_sr} -z ${npa_sr} \
  $node_file_raw \
  $edge_file_raw 2>"${SUCCINCT_LOG_PATH}/handler.log" >/dev/null &
  #2>&1 > "${SUCCINCT_LOG_PATH}/handler_${2}.log" &
  # NOTE: use the /dev/null version to pipe to each worker's local log (which won't be piped back to master)
