#!/bin/bash
source ./conf/succinct-env.sh

suffix=""
suffix="WithTsAttr"

partitionType=0 # 0 for edge table
partitionType=1 # 0 for node table

function par_create() {
  pushd data >/dev/null

  dataset=20attr35each
  dataset=40attr16each
 
  #assoc_file="higgs-social_network.opts-npa${npa}sa${sa}isa${isa}.assoc"
  #node_file="higgs-${dataset}-tpch-npa${npa}sa${sa}isa${isa}.node"
  #ln -sf higgs-social_network.assoc ${assoc_file}
  #ln -sf higgs-${dataset}-tpch.node ${node_file}
  assoc_file="/mnt2T/data/liveJournal-minDeg${minDeg}${suffix}-npa${npa}sa${sa}isa${isa}.assoc"
  node_file="/mnt2T/data/liveJournal-${dataset}-tpch-npa${npa}sa${sa}isa${isa}.node"
  ln -sf /mnt2T/data/liveJournal-minDeg${minDeg}${suffix}.assoc ${assoc_file}
  #ln -sf /mnt2T/data/liveJournal-${dataset}-tpch.node ${node_file}
 
  popd >/dev/null

  prog=./bin/graph-partitioner
  if [ ! -f "$prog" ]; then
    >&2 echo "Binary graph-partitioner hasn't been built yet: run \`make graph-partitioner\`?"
    exit 1
  fi

  source "./sbin/load-succinct-env.sh"

  $prog \
    -n $TOTAL_NUM_SHARDS \
    -t $partitionType \
    ${node_file} \
    ${assoc_file}
  echo "Partitioned '$node_file' and '$assoc_file' for $TOTAL_NUM_SHARDS shards."
}

for minDeg in 60; do
sa=32; isa=64; npa=128; par_create &
sa=8; isa=64; npa=64; par_create &
sa=4; isa=16; npa=16; par_create &
done

wait
