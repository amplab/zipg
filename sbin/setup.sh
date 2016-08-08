#!/bin/bash
sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/succinct-config.sh"
. "$sbin/load-succinct-env.sh"

npa=128; sa=32; isa=64 # L0, by default

if [ -f "$SUCCINCT_CONF_DIR/master" ]; then
  master=`cat "$SUCCINCT_CONF_DIR/master"`
else
  master="localhost"
fi

dataset=${1}

function timestamp() {
  date +"%D-%T"
}

function setup() {
  # By default disable strict host key checking
  if [ "$SUCCINCT_SSH_OPTS" = "" ]; then
    SUCCINCT_SSH_OPTS="-o StrictHostKeyChecking=no"
  fi

  if [ "$dataset" = "" ]; then
    echo "Must specify dataset"
  fi

  node_file_raw=/mnt2/${dataset}/${dataset}.node
  edge_file_raw=/mnt2/${dataset}/${dataset}.assoc

  if [ "$master" = "localhost" ]; then
    bash $sbin/../scripts/setup_dist.sh $node_file_raw $edge_file_raw $sa $isa $npa
  else
    ssh $SUCCINCT_SSH_OPTS "$master" "bash $sbin/../scripts/setup_dist.sh $node_file_raw $edge_file_raw $sa $isa $npa"
  fi
}

setup 
