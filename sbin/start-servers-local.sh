#!/usr/bin/env bash
# Launch logical shards locally on this physical node.


sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/succinct-config.sh"

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/succinct-config.sh"

. "$SUCCINCT_PREFIX/sbin/load-succinct-env.sh"

bin="$SUCCINCT_HOME/bin"
bin="`cd "$bin"; pwd`"

export LD_LIBRARY_PATH=$SUCCINCT_HOME/lib

############# usage 

# number of shards to launch on each physical node
num_shards=$1
# total number of physical nodes in cluster
num_hosts=$2
# which id identifies this physical node?
local_host_id=$3
# ??
num_replicas=$( wc -l < ${SUCCINCT_CONF_DIR}/repl)

if [ "$num_shards" = "" ]; then
  num_shards=1
fi

if [ "$num_hosts" = "" ]; then
  num_hosts=1
fi

if [ "$local_host_id" = "" ]; then
  local_host_id=1
fi

if [ "$SUCCINCT_DATA_PATH" = "" ]; then
  SUCCINCT_DATA_PATH="$SUCCINCT_HOME/data"
fi

if [ "$SUCCINCT_LOG_PATH" = "" ]; then
	SUCCINCT_LOG_PATH="$SUCCINCT_HOME/log"
fi

mkdir -p $SUCCINCT_LOG_PATH

if [ "$QUERY_SERVER_PORT" = "" ]; then
	QUERY_SERVER_PORT=11002
fi

i=0
while read sr dist; do
	((sampling_rates[$i] = $sr))
	i=$(($i + 1))
done < ${SUCCINCT_CONF_DIR}/repl

limit=$(($num_shards - 1))
padWidth=${#limit}

for i in `seq 0 $limit`; do
	port=$(($QUERY_SERVER_PORT + $i))
	shard_id=$(($i * $num_hosts + local_host_id)) # balance across physical nodes
    padded_shard_id=$(printf "%0*d" ${padWidth} ${shard_id})
    node_split="${NODE_FILE}-part${padded_shard_id}"
    edge_split="${EDGE_FILE}-part${padded_shard_id}"

    nohup "$bin/graph_query_server" \
      -m 0 \
      -p $port \
      $node_split \
      $edge_split \
      2>&1 >"$SUCCINCT_LOG_PATH/server_${i}.log" &

done
