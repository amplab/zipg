#!/usr/bin/env bash
sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/succinct-config.sh"

# If the hosts file is specified in the command line,
# then it takes precedence over the definition in
# succinct-env.sh. Save it here.
if [ -f "$SUCCINCT_HOSTS" ]; then
  HOSTLIST=`cat "$SUCCINCT_HOSTS"`
fi

if [ "$SHARDS_PER_SERVER" = "" ]; then
  SHARDS_PER_SERVER="1"
fi

. "$SUCCINCT_PREFIX/sbin/load-succinct-env.sh"

if [ "$HOSTLIST" = "" ]; then
  if [ "$SUCCINCT_HOSTS" = "" ]; then
    if [ -f "${SUCCINCT_CONF_DIR}/hosts" ]; then
      HOSTLIST=`cat "${SUCCINCT_CONF_DIR}/hosts"`
    else
      HOSTLIST=localhost
    fi
  else
    HOSTLIST=`cat "${SUCCINCT_HOSTS}"`
  fi
fi

# Launch the handlers
# By default disable strict host key checking
if [ "$SUCCINCT_SSH_OPTS" = "" ]; then
  SUCCINCT_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

# optionally support input file override
# default is to read from sbin/succinct-config.sh
node_file_raw=$1
if [ "$node_file_raw" = "" ]; then
  node_file_raw=${NODE_FILE}
fi
edge_file_raw=$2
if [ "$edge_file_raw" = "" ]; then
  edge_file_raw=${EDGE_FILE}
fi

i=0
for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
	# NOTE: $3 $4 $5 are supposed to be sampling rates
  ssh $SUCCINCT_SSH_OPTS "$host" "$sbin/start-handler.sh" $SHARDS_PER_SERVER $i \
    $node_file_raw \
    $edge_file_raw \
    $3 $4 $5 2>&1 | sed "s/^/$host: /" &
  i=$(( $i + 1 ))
done

$sbin/hosts.sh $sbin/check-started.sh