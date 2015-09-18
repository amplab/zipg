host_list_file=/home/ubuntu/hosts

usage="Usage: slaves.sh [--config <conf-dir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

# . "$sbin/spark-config.sh"

# If the slaves file is specified in the command line,
# then it takes precedence over the definition in
# spark-env.sh. Save it here.
if [ -f "$SPARK_SLAVES" ]; then
  HOSTLIST=`cat "$SPARK_SLAVES"`
fi
HOSTLIST=`cat ${host_list_file}`

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
    export SPARK_CONF_DIR="$conf_dir"
  fi
  shift
fi

# . "$SPARK_PREFIX/bin/load-spark-env.sh"
if [ "$HOSTLIST" = "" ]; then
  if [ "$SPARK_SLAVES" = "" ]; then
    if [ -f "${SPARK_CONF_DIR}/slaves" ]; then
      HOSTLIST=`cat "${SPARK_CONF_DIR}/slaves"`
    else
      HOSTLIST=localhost
    fi
  else
    HOSTLIST=`cat "${SPARK_SLAVES}"`
  fi
fi


# By default disable strict host key checking
if [ "$SPARK_SSH_OPTS" = "" ]; then
  SPARK_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

for slave in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  if [ -n "${SPARK_SSH_FOREGROUND}" ]; then
    ssh -i ~/.ssh/oregon.pem $SPARK_SSH_OPTS "$slave" $"${@// /\\ }" \
      2>&1 | sed "s/^/$slave: /"
  else
    ssh -i ~/.ssh/oregon.pem $SPARK_SSH_OPTS "$slave" $"${@// /\\ }" \
      2>&1 | sed "s/^/$slave: /" &
  fi
  if [ "$SPARK_SLAVE_SLEEP" != "" ]; then
    sleep $SPARK_SLAVE_SLEEP
  fi
done

wait
