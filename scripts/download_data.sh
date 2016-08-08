#!/bin/bash
set -ex

dataset=$1

currDir=$(cd $(dirname $0); pwd)
. "${currDir}/../sbin/succinct-config.sh"
. "${currDir}/../sbin/load-succinct-env.sh"

#### Copy the corresponding shard files over
echo "Copying shard files..."

num_succinctstore_hosts=$num_hosts

limit=$(($TOTAL_NUM_SHARDS - 1))
padWidth=${#TOTAL_NUM_SHARDS}

echo "Shard id range 0-$limit"
echo "Number of Succinct Store Hosts: $num_succinctstore_hosts"

echo "Hosts: $(cat ${currDir}/../conf/hosts)"

echo "Beginning to copy in 10s..."
sleep 10

for shard_id in `seq 0 $limit`; do
  # transfer shard id i to an appropriate host
  host_id=$(($shard_id % num_succinctstore_hosts))
  host=$(sed -n "$(($host_id + 1)){p;q;}" ${currDir}/../conf/hosts)

  padded_shard_id=$(printf "%0*d" ${padWidth} ${shard_id})
  nodeTblS3="s3://succinct-datasets/linkbench/succinct/${dataset}/distributed/${dataset}.node-part${padded_shard_id}of${TOTAL_NUM_SHARDS}WithPtrs.succinct"
  edgeTblS3="s3://succinct-datasets/linkbench/succinct/${dataset}/distributed/${dataset}.edge_table-part${padded_shard_id}of${TOTAL_NUM_SHARDS}.succinct"
  edgeDelS3="s3://succinct-datasets/linkbench/succinct/${dataset}/distributed/${dataset}.edge_table-part${padded_shard_id}of${TOTAL_NUM_SHARDS}.deletes"

  echo "shard_id: ${shard_id}, host_id ${host_id}, host ${host}"
  echo "nodeTblS3: $nodeTblS3"
  echo "edgeTblS3: $edgeTblS3"
  echo "edgeDelS3: $edgeDelS3"
  
  sleep 1

  ssh -o StrictHostKeyChecking=no $host mkdir -p /mnt2/$dataset/
  ssh -o StrictHostKeyChecking=no $host ~/s3cmd-master/s3cmd get --recursive $nodeTblS3 /mnt2/$dataset/ 2>&1 | sed "s/^/$host: /" &
  ssh -o StrictHostKeyChecking=no $host ~/s3cmd-master/s3cmd get --recursive $edgeTblS3 /mnt2/$dataset/ 2>&1 | sed "s/^/$host: /" &
  ssh -o StrictHostKeyChecking=no $host ~/s3cmd-master/s3cmd get --recursive $edgeDelS3 /mnt2/$dataset/ 2>&1 | sed "s/^/$host: /" &
done
wait
echo "Shard files downloaded at all servers."
