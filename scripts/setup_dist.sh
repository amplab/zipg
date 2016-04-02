#!/bin/bash
set -ex

npa=128; sa=32; isa=64 # L0, by default

#node_file_raw=/mnt2/uk-2007-05-40attr16each-tpch-npa128sa32isa64.node
#edge_file_raw=/mnt2/uk-2007-05-40attr16each-npa128sa32isa64.assoc
node_file_raw=/mnt2/twitter2010-40attr16each-tpch.node
edge_file_raw=/mnt2/twitter2010-npa128sa32isa64.assoc

currDir=$(cd $(dirname $0); pwd)
. "${currDir}/../sbin/succinct-config.sh"
. "${currDir}/../sbin/load-succinct-env.sh"

function stop_all() {
  bash ${currDir}/../sbin/stop-all.sh 
  sleep 2
}

function start_all() {
  ${currDir}/../sbin/start-servers.sh $node_file_raw $edge_file_raw $sa $isa $npa
  sleep 2

  ${currDir}/../sbin/start-handlers.sh 
  sleep 2

  ${currDir}/../sbin/load-data.sh
  sleep 2

  # note: the script launch order is important
  ${currDir}/../sbin/backfill-updates.sh
  sleep 2
}

function timestamp() {
  date +"%D-%T"
}

bash ${currDir}/../sbin/hosts.sh source "${currDir}/../sbin/succinct-config.sh"
bash ${currDir}/../sbin/hosts.sh source "${currDir}/../sbin/load-succinct-env.sh"
sleep 2

stop_all
start_all
