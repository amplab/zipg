#!/bin/bash
source ./conf/succinct-env.sh

function par_create() {
   pushd data >/dev/null
 
   assoc_file="higgs-social_network.opts-npa${npa}sa${sa}isa${isa}.assoc"
   node_file="higgs-2attr350each-tpch-npa${npa}sa${sa}isa${isa}.node"
   #node_file="higgs-25attr35each-tpch-npa${npa}sa${sa}isa${isa}.node"
 
   ln -sf higgs-social_network.assoc ${assoc_file}
   ln -sf higgs-2attr350each-tpch.node ${node_file}
   #ln -sf higgs-25attr35each-tpch.node ${node_file}
 
   popd >/dev/null


  prog=./bin/graph-partitioner
  if [ ! -f "$prog" ]; then
    >&2 echo "Binary graph-partitioner hasn't been built yet: run \`make graph-partitioner\`?"
    exit 1
  fi
  source "./sbin/load-succinct-env.sh"
#j  cmd="$prog \
#j    -n $TOTAL_NUM_SHARDS \
#j    ${node_file} \
#j    ${assoc_file}"
#j  echo "cmd: " $cmd

  $prog \
    -n $TOTAL_NUM_SHARDS \
    data/${node_file} \
    data/${assoc_file}

  echo "Partitioned '$node_file' and '$assoc_file' for $TOTAL_NUM_SHARDS shards."
 
   #./bin/create graph-construct data/${node_file} data/${assoc_file} ${sa} ${isa} ${npa} &
}
sa=8
isa=64
npa=64
par_create &

#sa=32
#isa=64
#npa=128
#par_create

sa=4
isa=16
npa=16
par_create &
wait

#npa=32
#isa=32
#for sa in 2 4 8 16 32; do 
#  pushd data >/dev/null
#
#  assoc_file="higgs-social_network.opts-npa${npa}sa${sa}isa${isa}.assoc"
#  node_file="higgs-2attr350each-tpch-npa${npa}sa${sa}isa${isa}.node"
#
#  ln -sf higgs-social_network.lessPad.noPadHeader.assoc ${assoc_file}
#  ln -sf higgs-2attr350each-tpch.node ${node_file}
#
#  popd >/dev/null
#
#  ./bin/create graph-construct data/${node_file} data/${assoc_file} ${sa} ${isa} ${npa} &
#done
#
#wait

#npa=32
#sa=32
#for isa in 2 4 8 16 32; do 
#  pushd data >/dev/null
#
#  assoc_file="higgs-social_network.opts-npa${npa}sa${sa}isa${isa}.assoc"
#  node_file="higgs-2attr350each-tpch-npa${npa}sa${sa}isa${isa}.node"
#
#  ln -sf higgs-social_network.lessPad.noPadHeader.assoc ${assoc_file}
#  ln -sf higgs-2attr350each-tpch.node ${node_file}
#
#  popd >/dev/null
#
#  ./bin/create graph-construct data/${node_file} data/${assoc_file} ${sa} ${isa} ${npa} &
#done
#wait

#isa=32
#sa=32
#for npa in 8 16 32 64; do 
#  pushd data >/dev/null
#
#  assoc_file="higgs-social_network.opts-npa${npa}sa${sa}isa${isa}.assoc"
#  node_file="higgs-2attr350each-tpch-npa${npa}sa${sa}isa${isa}.node"
#
#  ln -sf higgs-social_network.lessPad.noPadHeader.assoc ${assoc_file}
#  ln -sf higgs-2attr350each-tpch.node ${node_file}
#
#  popd >/dev/null
#
#  ./bin/create graph-construct data/${node_file} data/${assoc_file} ${sa} ${isa} ${npa} &
#done
# wait
