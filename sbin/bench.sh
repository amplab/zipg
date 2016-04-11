#!/bin/bash
set -ex

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

datasets=(
  uk
  twitter
)

threads=( 56 )
benches=(
  benchTaoMix
  benchTaoUpdates # latency
  #benchTaoMixThput
  #benchTaoMixWithUpdatesThput
  #benchMixThput
  #benchNhbrNodeThput
  #benchNeighborThput
  #benchNhbrAtypeThput
  #benchNodeNodeThput
  #benchEdgeAttrsThput
)

function timestamp() {
  date +"%D-%T"
}

function setup() {
  # By default disable strict host key checking
  if [ "$SUCCINCT_SSH_OPTS" = "" ]; then
    SUCCINCT_SSH_OPTS="-o StrictHostKeyChecking=no -i $SUCCINCT_CONF_DIR/cqlkeypair.pem"
  fi

  if [ "$dataset" = "twitter" ]; then
    node_file_raw=/mnt2/twitter2010-40attr16each-tpch.node
    edge_file_raw=/mnt2/twitter2010-npa128sa32isa64.assoc
  elif [ "$dataset" = "uk" ]; then
    node_file_raw=/mnt2/uk-2007-05-40attr16each-tpch.node
    edge_file_raw=/mnt2/uk-2007-05-40attr16each-npa128sa32isa64.assoc
  else
    echo "Must specify dataset."
    exit
  fi
    
  ssh $SUCCINCT_SSH_OPTS "$master" "bash $sbin/../scripts/setup_dist.sh $node_file_raw $edge_file_raw $sa $isa $npa"
}

function bench_latency() {
  client=`tail -n 1 "$SUCCINCT_CONF_DIR/servers"`
  
  # By default disable strict host key checking
  if [ "$SUCCINCT_SSH_OPTS" = "" ]; then
    SUCCINCT_SSH_OPTS="-o StrictHostKeyChecking=no -i $SUCCINCT_CONF_DIR/cqlkeypair.pem"
  fi

  ssh $SUCCINCT_SSH_OPTS "$client" "bash $benchType=T && bash ${currDir}/../scripts/bench_func.sh $node_file_raw $edge_file_raw 0 localhost false 2>&1"
}

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
    bash $sbin/hosts.sh \
      rm -rf throughput_${bench}-npa128sa32isa64-${throughput_threads}clients.txt
  done
done

for benchType in "${benches[@]}"; do
  case $benchType in
    *Thput)
      for throughput_threads in ${threads[*]}; do
        echo "Running throughput benchmark, ${benchType}, numThreads=$throughput_threads"
        setup

        launcherStart=$(date +"%s")

        bash $sbin/hosts-bench.sh $node_file_raw $edge_file_raw $throughput_threads $benchType
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
      done
      ;;
    *)
      echo "Running latency benchmark, ${benchType}"
      setup

      # launch a single client on the last node of the cluster
      bench_latency
      wait
      ;;
  esac
done
