#!/bin/bash
set -e

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"
source "$sbin/succinct-config.sh"
source "$SUCCINCT_PREFIX/sbin/load-succinct-env.sh"

prog=$SUCCINCT_HOME/bin/graph-partitioner
if [ ! -f "$prog" ]; then
  >&2 echo "Binary graph-partitioner hasn't been built yet: run \`make graph-partitioner\`?"
  exit 1
fi

$prog \
  -n $TOTAL_NUM_SHARDS \
  $NODE_FILE \
  $EDGE_FILE

echo "Partitioned '$NODE_FILE' and '$EDGE_FILE' for $TOTAL_NUM_SHARDS shards."
