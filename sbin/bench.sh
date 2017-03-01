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
  #uk
  twitter
  #orkut
)
threads=( 64 128 256 )
benches=(
  #benchNhbrNode # latency
  #benchNhbr # latency
  #benchNhbrAtype # latency
  #benchNodeNode # latency
  #benchEdgeAttrs # latency
  #benchPrimitiveMix # latency
  #benchTaoAssocRange # latency
  #benchTaoAssocCount # latency
  #benchTaoObjGet # latency
  #benchTaoAssocGet # latency
  #benchTaoAssocTimeRange # latency
  #benchTaoAssocAdd # latency
  #benchTaoObjAdd # latency
  #benchTaoMix  # latency
  #benchTaoMixWithUpdates # latency
  benchNhbrNodeThput
  benchNhbrNode2Thput
  benchNodeNodeThput
  benchNodeNode2Thput
  #benchPrimitiveMixThput
  #benchTaoMixThput
  #benchTaoMixWithUpdatesThput
  #benchNhbrNodeThput
  #benchNhbrThput
  #benchNhbrAtypeThput
  #benchNodeNodeThput
  #benchEdgeAttrsThput
  #benchTaoAssocRangeThput
  #benchTaoAssocCountThput
  #benchTaoObjGetThput
  #benchTaoAssocGetThput
  #benchTaoAssocTimeRangeThput
  #benchTaoAssocAddThput
  #benchTaoObjAddThput
)

function timestamp() {
  date +"%D-%T"
}

function setup() {
  # By default disable strict host key checking
  if [ "$SUCCINCT_SSH_OPTS" = "" ]; then
    SUCCINCT_SSH_OPTS="-o StrictHostKeyChecking=no"
  fi

  if [ "$dataset" = "twitter" ]; then
    node_file_raw=$HOME/data/twitter/twitter.node
    edge_file_raw=$HOME/data/twitter/twitter.assoc
    $sbin/hosts.sh rm -rf $HOME/queries
    $sbin/hosts.sh cp -r $HOME/twitterQueries $HOME/queries
  elif [ "$dataset" = "uk" ]; then
    node_file_raw=$HOME/data/uk/uk.node
    edge_file_raw=$HOME/data/uk/uk.assoc
    $sbin/hosts.sh rm -rf $HOME/queries
    $sbin/hosts.sh cp -r $HOME/ukQueries $HOME/queries
  elif [ "$dataset" = "orkut" ]; then
    node_file_raw=$HOME/data/orkut/orkut.node
    edge_file_raw=$HOME/data/orkut/orkut.assoc
    $sbin/hosts.sh rm -rf $HOME/queries
    $sbin/hosts.sh cp -r $HOME/orkutQueries $HOME/queries
  else
    echo "Must specify dataset."
    exit
  fi
	
  if [ "$master" = "localhost" ]; then
    bash $sbin/../scripts/setup_dist.sh $node_file_raw $edge_file_raw $sa $isa $npa
  else
    ssh $SUCCINCT_SSH_OPTS "$master" "bash $sbin/../scripts/setup_dist.sh $node_file_raw $edge_file_raw $sa $isa $npa"
  fi
}

function bench_latency() {

  if [ "$dataset" = "" ]; then
    echo "Must specify dataset."
    exit
  fi
	
  client=`tail -n 1 "$SUCCINCT_CONF_DIR/servers"`
  
  # By default disable strict host key checking
  if [ "$SUCCINCT_SSH_OPTS" = "" ]; then
    if [ "$client" = "localhost" ]; then
      SUCCINCT_SSH_OPTS="-o StrictHostKeyChecking=no"
    else
      SUCCINCT_SSH_OPTS="-o StrictHostKeyChecking=no -i $SUCCINCT_CONF_DIR/cqlkeypair.pem"
    fi
  fi
	
  ssh $SUCCINCT_SSH_OPTS "$client" "rm -rf $HOME/queries"
  scp $SUCCINCT_SSH_OPTS -r $HOME/${dataset}Queries $client:$HOME/queries
  ssh $SUCCINCT_SSH_OPTS "$client" "$benchType=T bash ${currDir}/../scripts/bench_func.sh $node_file_raw $edge_file_raw 0 localhost false $sa $isa $npa $dataset 2>&1"
}

#### Launch benchmark
declare -A benchMap=(
  ["benchTaoMixThput"]="tao_mix"
  ["benchTaoMixWithUpdatesThput"]="taoMixWithUpdates"
  ["benchPrimitiveMixThput"]="mix"
  ["benchNodeNodeThput"]="get_nodes2"
  ["benchNodeNode2Thput"]="get_nodes22"
  ["benchNhbrNodeThput"]="get_nhbrsNode"
  ["benchNhbrNode2Thput"]="get_nhbrsNode2"
  ["benchEdgeAttrsThput"]="getEdgeAttrs"
  ["benchNhbrAtypeThput"]="get_nhbrsAtype"
  ["benchNhbrThput"]="get_nhbrs"
  ["benchTaoAssocGetThput"]="tao_assoc_get"
  ["benchTaoAssocCountThput"]="tao_assoc_count"
  ["benchTaoAssocRangeThput"]="tao_assoc_range"
  ["benchTaoAssocTimeRangeThput"]="tao_assoc_time_range"
  ["benchTaoObjGetThput"]="tao_obj_get"
)

for throughput_threads in ${threads[*]}; do
  for bench in get_nodes2 get_nodes22 get_nhbrsNode get_nhbrsNode2 get_nhbrsAtype getEdgeAttrs get_nhbrs tao_mix mix taoMixWithUpdates; do
    bash $sbin/hosts.sh \
      rm -rf throughput_${bench}-npa128sa32isa64-${throughput_threads}clients.txt
  done
done

for dataset in "${datasets[@]}"; do
  for benchType in "${benches[@]}"; do
    case $benchType in
      *Thput)
        for throughput_threads in ${threads[*]}; do
          echo "Running throughput benchmark, ${benchType}, numThreads=$throughput_threads"
          setup

          launcherStart=$(date +"%s")

          bash $sbin/hosts-bench.sh $node_file_raw $edge_file_raw $throughput_threads $benchType $sa $isa $npa $dataset
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
done
