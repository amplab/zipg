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

#node_file_raw=/mnt2/uk-2007-05-40attr16each-tpch-npa128sa32isa64.node
#edge_file_raw=/mnt2/uk-2007-05-40attr16each-npa128sa32isa64.assoc
node_file_raw=/mnt2/twitter2010-40attr16each-tpch.node
edge_file_raw=/mnt2/twitter2010-npa128sa32isa64.assoc

threads=( 16 32 64 )
benches=(
  #benchTaoMix
  #benchTaoUpdates # latency
  benchTaoMixThput
  #benchTaoMixWithUpdatesThput
  #benchMixThput
  #benchNhbrNodeThput
  #benchNeighborThput
  #benchNhbrAtypeThput
  #benchNodeNodeThput
  #benchEdgeAttrsThput
)

currDir=$(cd $(dirname $0); pwd)
. "${currDir}/../sbin/succinct-config.sh"
. "${currDir}/../sbin/load-succinct-env.sh"

#### Launch aggregator & shards on all hosts

function timestamp() {
  date +"%D-%T"
}

bash ${currDir}/../sbin/hosts.sh source "${currDir}/../sbin/succinct-config.sh"
bash ${currDir}/../sbin/hosts.sh source "${currDir}/../sbin/load-succinct-env.sh"
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
      done
      ;;
    *)
      echo "Running latency benchmark, ${benchType}"
      stop_all
      start_all

      # launch the single client from this master
      export $benchType=T && bash ${currDir}/bench_func.sh \
        $node_file_raw $edge_file_raw $throughput_threads localhost false 2>&1 >run.log
      wait
      ;;
  esac
done
