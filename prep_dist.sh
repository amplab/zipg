#!/bin/bash
set -e

NDEBUG=-DNDEBUG # use this for last runs
NDEBUG="" # use this for testing

node_file_raw=my.node
edge_split=my.assoc

#### Initial setup

ln -sf ~/spark-ec2/slaves conf/hosts

currDir=$(cd $(dirname $0); pwd)
. "${currDir}/succinct-config.sh"
. "${currDir}/load-succinct-env.sh"

echo "Num shards: ${TOTAL_NUM_SHARDS}; Num hosts: ${num_hosts}"

SGFLAGS="${NDEBUG}" cmake .
make -j
sbin/copy-dir --delete ./
sbin/copy-dir --delete ~/.bashrc
sbin/copy-dir --delete ~/.gitconfig

#### Copy the corresponding shard files over

limit=$(($TOTAL_NUM_SHARDS - 1))
padWidth=${#limit}
paddedTotalNumShards=$(printf "%0*d" ${padWidth} ${TOTAL_NUM_SHARDS})
for shard_id in `seq 0 $limit`; do
    # transfer shard id i to an appropriate host
    host_id=$(($shard_id % num_hosts))
    host=$(sed -n "${host_id}{p;q;}" ${currDir}/conf/hosts)

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

    rsync ${nodeTbl} ${host}:${nodeTbl} &
    rsync ${edgeTbl} ${host}:${edgeTbl} &
done
wait
echo "Shard files copied to all servers."
