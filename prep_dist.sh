#!/bin/bash
set -e

#### Steps:
# 1. modify the configs
# 2. bash ${currDir}/sbin/hosts.sh ${currDir}/build_thrift.sh &
# 3. build successfully on this machine:
#       ./build_thrift.sh && cmake . && make -j

node_file_raw=/vol0/orkut-40attr16each-tpch-npa128sa32isa64.node
edge_file_raw=/vol0/orkut-40attr16each-npa128sa32isa64.assoc

# L0, by default
npa=128
sa=32
isa=64

copyShardFiles=T

#### Initial setup

yes | cp ~/spark-ec2/slaves conf/hosts >/dev/null

currDir=$(cd $(dirname $0); pwd)
. "${currDir}/sbin/succinct-config.sh"
. "${currDir}/sbin/load-succinct-env.sh"

echo "Num shards: ${TOTAL_NUM_SHARDS}; Num hosts: ${num_hosts}"

sbin/copy-dir --delete ./
sbin/copy-dir --delete ~/.bashrc
sbin/copy-dir --delete ~/.gitconfig

#### Copy the corresponding shard files over
if [[ -n $copyShardFiles ]]; then
  limit=$(($TOTAL_NUM_SHARDS - 1))
  padWidth=${#limit}
  paddedTotalNumShards=$(printf "%0*d" ${padWidth} ${TOTAL_NUM_SHARDS})
  for shard_id in `seq 0 $limit`; do
      # transfer shard id i to an appropriate host
      host_id=$(($shard_id % num_hosts))
      host=$(sed -n "$(($host_id + 1)){p;q;}" ${currDir}/conf/hosts)
  
      padded_shard_id=$(printf "%0*d" ${padWidth} ${shard_id})
      node_split="${node_file_raw}-part${padded_shard_id}of${paddedTotalNumShards}"
      edge_split="${edge_file_raw}-part${padded_shard_id}of${paddedTotalNumShards}"
  
      # Encoded succinct dirs
      # Hacky: note this uses internal impl details about namings of encoded tables
      nodeTbl="${node_split}WithPtrs.succinct"
      # This replaces the last 'assoc' by 'edge_table'
      edgeTbl=$(echo -n "${edge_split}.succinct" | sed 's/\(.*\)assoc\(.*\)/\1edge_table\2/')
  
      if [[ ! -d "${nodeTbl}" ]]; then
        echo "dir ${nodeTbl} doesn't exist, exiting"
        exit 1
      fi
      if [[ ! -d "${edgeTbl}" ]]; then
        echo "dir ${edgeTbl} doesn't exist, exiting"
        exit 1
      fi
  
      echo "shard_id: ${shard_id}, host_id ${host_id}"
  
      rsync -ar ${nodeTbl} ${host}:${nodeTbl} &
      rsync -ar ${edgeTbl} ${host}:${edgeTbl} &
  done
  wait
  echo "Shard files copied to all servers."
fi
wait

#### Launch aggregator & shards on all hosts
bash ${currDir}/sbin/hosts.sh ${currDir}/sbin/stop-all.sh
sleep 2

bash ${currDir}/sbin/hosts.sh source "${currDir}/sbin/succinct-config.sh"
bash ${currDir}/sbin/hosts.sh source "${currDir}/sbin/load-succinct-env.sh"
sleep 2

bash ${currDir}/sbin/start-servers.sh $node_file_raw $edge_file_raw $sa $isa $npa &
sleep 2

bash ${currDir}/sbin/start-handlers.sh &
sleep 2

#### Launch benchmark
# TODO
