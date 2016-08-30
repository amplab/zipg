#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

query_types=(
addlink
deletelink
updatelink
countlink
getlink
getlinklist
getnode
addnode
updatenode
deletenode
)

read_only=(
#countlink
#getlink
getlinklist
#getnode
)

function qopts() {
  query=$1
  QOPTS_=""
  for query_type in ${query_types[@]}; do
    if [ "$query" = "$query_type" ]; then
      QOPTS_="$QOPTS_ -D$query_type=100"
    else
      QOPTS_="$QOPTS_ -D$query_type=0"
    fi
  done
  echo $QOPTS_
}

function setup() {
  dataset=$1
  zipg_master=`cat ${sbin}/zipg_master`

  # By default disable strict CLIENT key checking
  if [ "$SSH_OPTS" = "" ]; then
    SSH_OPTS="-o StrictHostKeyChecking=no"
  fi

  ssh $SSH_OPTS "$zipg_master" ~/succinct-graph/sbin/setup.sh $dataset 2>&1 | sed "s/^/$zipg_master: /"
}

echo "Copying benchmark directory"
$sbin/copy-dir $sbin/../

dataset=$1

for query_type in ${read_only[@]}; do
  QOPTS=`qopts $query_type`
  for num_threads in 32; do
    setup $dataset
    echo "Setup complete"
    sleep 1
    $sbin/bench_on_clients.sh $dataset $num_threads $QOPTS
  done
done
