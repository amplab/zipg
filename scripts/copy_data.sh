#!/bin/bash
set -ex

#node_file_raw=/mnt2/uk-2007-05-40attr16each-tpch-npa128sa32isa64.node
#edge_file_raw=/mnt2/uk-2007-05-40attr16each-npa128sa32isa64.assoc
node_file_raw=/mnt2/twitter2010-40attr16each-tpch.node
edge_file_raw=/mnt2/twitter2010-npa128sa32isa64.assoc

currDir=$(cd $(dirname $0); pwd)
. "${currDir}/../sbin/succinct-config.sh"
. "${currDir}/../sbin/load-succinct-env.sh"

#### Copy the corresponding shard files over
echo "Copying shard files..."

num_succinctstore_hosts=$num_hosts
if [[ "$ENABLE_MULTI_STORE" == T ]]; then
  num_succinctstore_hosts=$(( num_hosts - 2 ))
fi

limit=$(($TOTAL_NUM_SHARDS - 1))
padWidth=${#TOTAL_NUM_SHARDS}

echo "Shard id range 0-$limit"
echo "Number of Succinct Store Hosts: $num_succinctstore_hosts"

echo "Beginning to copy in 10s..."
sleep 10

for shard_id in `seq 0 $limit`; do
  # transfer shard id i to an appropriate host
  host_id=$(($shard_id % num_succinctstore_hosts))
  host=$(sed -n "$(($host_id + 1)){p;q;}" ${currDir}/../conf/hosts)

  padded_shard_id=$(printf "%0*d" ${padWidth} ${shard_id})
  node_split="${node_file_raw}-part${padded_shard_id}of${TOTAL_NUM_SHARDS}"
  edge_split="${edge_file_raw}-part${padded_shard_id}of${TOTAL_NUM_SHARDS}"

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

  d1=$(dirname "${nodeTbl}")
  d2=$(dirname "${edgeTbl}")
  rsync -arL ${nodeTbl} ${host}:$d1 &
  rsync -arL ${edgeTbl} ${host}:$d2 &
done

if [[ "$ENABLE_MULTI_STORE" == T ]]; then
  # copy stuff to SuffixStore mc and LogStore mc
  limit=$((NUM_SUFFIXSTORE_PARTS - 1))
  host=$(sed -n "$((num_succinctstore_hosts + 1)){p;q;}" ${currDir}/../conf/hosts)
  for i in $(seq 0 "${limit}"); do
    padWidth=${#NUM_SUFFIXSTORE_PARTS}
    padded_shard_id=$(printf "%0*d" ${padWidth} ${i})
    nodeTbl="${node_file_raw}.suffixstore-part${padded_shard_id}of${NUM_SUFFIXSTORE_PARTS}_suffixstore"
    edgeTbl="${edge_file_raw}.suffixstore-part${padded_shard_id}of${NUM_SUFFIXSTORE_PARTS}_suffixstore"
    d1=$(dirname "${nodeTbl}")
    d2=$(dirname "${edgeTbl}")
    if [ -e "$nodeTbl" ]; then
      rsync -arL ${nodeTbl} ${host}:$d1 &
    else
      echo "WARNING: SuffixStore node table not found"
    fi

    if [ -e "$edgeTbl" ]; then
      rsync -arL ${edgeTbl} ${host}:$d2 &
    else
      echo "WARNING: SuffixStore edge table not found"
    fi
    
    # also rsync the raw assoc input, useful for calculating updates; TODO: node as well
    edgeTbl="${edge_file_raw}.suffixstore-part${padded_shard_id}of${NUM_SUFFIXSTORE_PARTS}"
    rsync -arL ${edgeTbl} ${host}:$d2 &
  done

  limit=$((NUM_LOGSTORE_PARTS - 1))
  # Copy to last succinct store server
  host=$(sed -n "$((num_succinctstore_hosts + 2)){p;q;}" ${currDir}/../conf/hosts)
  for i in $(seq 0 "${limit}"); do
    padWidth=${#NUM_LOGSTORE_PARTS}
    padded_shard_id=$(printf "%0*d" ${padWidth} ${i})
    nodeTbl="${node_file_raw}.logstore-part${padded_shard_id}of${NUM_LOGSTORE_PARTS}_logstore"
    edgeTbl="${edge_file_raw}.logstore-part${padded_shard_id}of${NUM_LOGSTORE_PARTS}_logstore"
    d1=$(dirname "${nodeTbl}")
    d2=$(dirname "${edgeTbl}")
    if [ -e "$nodeTbl" ]; then
      rsync -arL ${nodeTbl} ${host}:$d1 &
    else
      echo "WARNING: LogStore node table not found"
    fi

    if [ -e "$edgeTbl" ]; then
      rsync -arL ${edgeTbl} ${host}:$d2 &
    else
      echo "WARNING: LogStore edge table not found"
    fi
  done
fi

wait
echo "Shard files copied to all servers."
