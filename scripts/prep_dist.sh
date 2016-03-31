#!/bin/bash
set -ex

#### Steps:
# 1. modify the configs
# 2. bash init.sh
# FIXME: the manual installation somehow doesn't work; use 0.9.2
# 3. bash sbin/hosts-sudo.sh sudo bash /vol0/succinct-graph/build_thrift.sh
# 4. build on this machine as well:
#       sudo bash ./build_thrift.sh && cmake . && make -j
# 5. Set the desired settings in rates-bench.sh

npa=128; sa=32; isa=64 # L0, by default
copyShardFiles=T

#node_file_raw=/mnt2/uk-2007-05-40attr16each-tpch-npa128sa32isa64.node
#edge_file_raw=/mnt2/uk-2007-05-40attr16each-npa128sa32isa64.assoc
node_file_raw=/mnt2/twitter2010-40attr16each-tpch.node
edge_file_raw=/mnt2/twitter2010-npa128sa32isa64.assoc

threads=( 64 )
benches=(
  #benchTaoMix
  #benchTaoUpdates # latency
  benchTaoMixThput
  benchTaoMixWithUpdatesThput
  #benchMixThput
  #benchNhbrNodeThput
  #benchNeighborThput
  #benchNhbrAtypeThput
  #benchNodeNodeThput
  #benchEdgeAttrsThput
)

# NOTE: settings here only affects this master killing all
# servers when the time's up.  The real times should be set
# in the code (e.g. GraphBenchmark), and recompile accordingly.
# in secs
# thputWarm=60
# thputMeasure=180
# thputCool=30

#### Initial setup

currDir=$(cd $(dirname $0); pwd)
. "${currDir}/../sbin/succinct-config.sh"
. "${currDir}/../sbin/load-succinct-env.sh"

#### Copy the corresponding shard files over
if [[ -n $copyShardFiles ]]; then
  echo "Copying shard files..."
  num_succinctstore_hosts=$num_hosts

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
    # copy logstore data
    limit=$((NUM_LOGSTORE_PARTS - 1))
    # Copy to last succinct store server
    host=$(sed -n "$((num_succinctstore_hosts)){p;q;}" ${currDir}/../conf/hosts)
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
fi

#### Launch aggregator & shards on all hosts

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

stop_all

bash ${currDir}/../sbin/hosts.sh source "${currDir}/sbin/succinct-config.sh"
bash ${currDir}/../sbin/hosts.sh source "${currDir}/sbin/load-succinct-env.sh"
sleep 2

#### Launch benchmark
declare -A benchMap=(
  ["benchTaoMixThput"]="tao_mix"
  ["benchTaoMixWithUpdatesThput"]="taoMixWithUpdates"
  ["benchTaoUpdates"]="taoUpdates"
  ["benchMixThput"]="mix"
  ["benchNodeNodeThput"]="get_nodes2"
  ["benchNhbrNodeThput"]="get_nhbrsNode"
  ["benchEdgeAttrsThput"]="getEdgeAttrs"
  ["benchNhbrAtypeThput"]="get_nhbrsAtype"
  ["benchNeighborThput"]="get_nhbrs"
)

for throughput_threads in ${threads[*]}; do
    for bench in get_nodes2 get_nhbrsNode get_nhbrsAtype getEdgeAttrs get_nhbrs tao_mix mix taoMixWithUpdates; do
      bash ${currDir}/../sbin/hosts.sh \
        rm -rf throughput_${bench}-npa128sa32isa64-${throughput_threads}clients.txt
    done
done

for benchType in "${benches[@]}"; do
  case $benchType in
    *Thput)
      for throughput_threads in ${threads[*]}; do
          start_all

          launcherStart=$(date +"%s")
          bash ${currDir}/../sbin/hosts-noStderr.sh \
            $benchType=T bash ${currDir}/bench_func.sh \
            $node_file_raw $edge_file_raw $throughput_threads localhost true 2>&1 >run.log
          wait
          launcherEnd=$(date +"%s")
          
          bench="${benchMap["$benchType"]}"
          rm -rf thput
          bash ${currDir}/../sbin/hosts.sh \
            tail -n1 throughput_${bench}-npa128sa32isa64-${throughput_threads}clients.txt | \
            cut -d',' -f2 | \
            cut -d' ' -f2 >>thput
          sum=$(awk '{ sum += $1 } END { print sum }' thput)

          f="thput-${bench}-${throughput_threads}clients.txt"
          t=$(timestamp)
          echo "$t,$bench" >>${f}
          sanity=$(${currDir}/../sbin/hosts.sh tail -n1 run.log)
          echo $sanity >> ${f}
          echo "Measured from master: $((launcherEnd - launcherStart)) secs" >> ${f}
          cat thput >> ${f}

          entry="$t,$bench,${throughput_threads}*10,$sum"
          echo $entry
          echo $entry >> thput-summary

          stop_all
      done
      ;;
    *)
      echo "Running latency benchmark, ${benchType}"
      start_all

      # launch the single client from this master
      export $benchType=T && bash ${currDir}/bench_func.sh \
        $node_file_raw $edge_file_raw $throughput_threads localhost false 2>&1 >run.log
      wait

      stop_all
      ;;
  esac
done
