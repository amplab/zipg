#!/bin/bash
set -e

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

# NOTE: this assumes the hostnames are ordered in the exact 
# same way as time order. In practice, we rely on the second
# to last machine having the SuffixStores, the last machine
# having the LogStores, etc.
for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  ssh $SUCCINCT_SSH_OPTS "$host" "$sbin/launch-backfill-updates.sh" 2>&1 | sed "s/^/$host: /" &
done
echo "Waiting for backfill updates"
wait
echo "backfill updates done"
