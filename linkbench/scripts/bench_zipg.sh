#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

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

for num_threads in 64; do
  setup $1
  $sbin/bench_on_clients.sh $dataset $num_threads
done
