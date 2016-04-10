#!/bin/bash
set -ex

npa=128; sa=32; isa=64 # L0, by default

#node_file_raw=/mnt2/uk-2007-05-40attr16each-tpch-npa128sa32isa64.node
#edge_file_raw=/mnt2/uk-2007-05-40attr16each-npa128sa32isa64.assoc
node_file_raw=/mnt2/twitter2010-40attr16each-tpch.node
edge_file_raw=/mnt2/twitter2010-npa128sa32isa64.assoc

currDir=$(cd $(dirname $0); pwd)

function stop_all() {
  bash ${currDir}/../sbin/stop-all.sh 
  sleep 2
}

function start_all() {
  bash ${currDir}/../sbin/start-handlers.sh $node_file_raw $edge_file_raw $sa $isa $npa 
  sleep 2
}

stop_all
start_all
