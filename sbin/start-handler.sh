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

mkdir -p $SUCCINCT_LOG_PATH

nohup "${bin}/../rpc/bin/graph_query_aggregator" \
  -t $TOTAL_NUM_SHARDS \
  -s $1 \
  -i $2 \
  -h "${SUCCINCT_CONF_DIR}/hosts" \
  -m "${ENABLE_MULTI_STORE}" \
  -f "${NUM_SUFFIXSTORE_PARTS}" \
  -l "${NUM_LOGSTORE_PARTS}" \
  2>&1 > "${SUCCINCT_LOG_PATH}/handler_${2}.log" &
  #2>"${SUCCINCT_LOG_PATH}/handler_${2}.log" >/dev/null &
