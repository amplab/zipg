# Run a shell command on all hosts.
#
# Environment Variables
#
#   SUCCINCT_HOSTS    File naming remote hosts.
#     Default is ${SUCCINCT_CONF_DIR}/hosts.
#   SUCCINCT_CONF_DIR  Alternate conf dir. Default is ${SUCCINCT_HOME}/conf.
#   SUCCINCT_HOST_SLEEP Seconds to sleep between spawning remote commands.
#   SUCCINCT_SSH_OPTS Options passed to ssh when running remote commands.
##

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/succinct-config.sh"

# If the hosts file is specified in the command line,
# then it takes precedence over the definition in
# succinct-env.sh. Save it here.
if [ -f "$SUCCINCT_HOSTS" ]; then
  HOSTLIST=`cat "$SUCCINCT_HOSTS"`
fi

# Check if --config is passed as an argument. It is an optional parameter.
# Exit if the argument is not a directory.
if [ "$1" == "--config" ]
then
  shift
  conf_dir="$1"
  if [ ! -d "$conf_dir" ]
  then
    echo "ERROR : $conf_dir is not a directory"
    echo $usage
    exit 1
  else
    export SUCCINCT_CONF_DIR="$conf_dir"
  fi
  shift
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

if [ -f "${SUCCINCT_CONF_DIR}/servers" ]; then
  SERVERLIST=`cat "${SUCCINCT_CONF_DIR}/servers"`
else
  SERVERLIST=$HOSTLIST
fi

# By default disable strict host key checking
if [ "$SUCCINCT_SSH_OPTS" = "" ]; then
  SUCCINCT_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

IFS=$'\n' read -r -d '' -a servers <<< "$SERVERLIST"
#`echo "$SERVERLIST"| sed "s/#.*$//;/^$/d"`
node_file_raw=$1
edge_file_raw=$2
throughput_threads=$3
benchType=$4
sa=$5
isa=$6
npa=$7
dataset=$8
i=0
for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  server=${servers[$i]}
  ssh $SUCCINCT_SSH_OPTS "$host" "$benchType=T bash $sbin/../scripts/bench_func.sh $node_file_raw $edge_file_raw $throughput_threads $server true $sa $isa $npa $dataset 2>&1 >run.log" &
  i=$((i + 1))
done

wait
