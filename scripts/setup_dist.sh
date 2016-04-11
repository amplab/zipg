#!/bin/bash
set -ex

npa=128; sa=32; isa=64 # L0, by default

node_file_raw=${1:-"data/data.node"}
edge_file_raw=${2:-"data/data.assoc"}
sa=${3:-"32"}
isa=${4:-"64"}
npa=${5:-"128"}

echo "Node File Prefix: $node_file_raw"
echo "Edge File Prefix: $edge_file_raw"
echo "Sampling Rates for SA: $sa, ISA: $isa, NPA: $npa"

currDir=$(cd $(dirname $0); pwd)

function stop_all() {
  bash ${currDir}/../sbin/stop-all.sh 
  sleep 2
}

function start_all() {
  bash ${currDir}/../sbin/start-servers.sh $node_file_raw $edge_file_raw $sa $isa $npa
  sleep 2

  bash ${currDir}/../sbin/start-handlers.sh 
  sleep 2

  bash ${currDir}/../sbin/load-data.sh
  sleep 2
}

stop_all
start_all
