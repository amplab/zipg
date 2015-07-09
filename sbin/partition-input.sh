#!/bin/bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"
source "$sbin/succinct-config.sh"
source "$SUCCINCT_PREFIX/sbin/load-succinct-env.sh"

$SUCCINCT_HOME/bin/graph-partitioner \
  -n $TOTAL_NUM_SHARDS \
  $NODE_FILE \
  $EDGE_FILE

echo "Partitioned '$NODE_FILE' and '$EDGE_FILE' for $TOTAL_NUM_SHARDS shards."
