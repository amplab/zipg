#!/bin/bash
set -e

# usage: ./bin/graph_encoder <maxConcurrentThreads> <sa> <isa> <npa> [files]+

function encode() {
  cmd="./bin/graph_encoder \
    3 \
    $sa \
    $isa \
    $npa \
    /mnt2T/data/live*${deg}WithTsAttr*npa${npa}*assoc*part*of8"
  #echo $cmd

  for i in `seq 0 1 7`; do
    cmd="ln -sf \
      /mnt2T/data/live*al-minDeg${deg}WithTsAttr*npa${npa}*assoc*part*of8 \
      /mnt2T/data/liveJournal-augOpts-minDeg${deg}-npa${npa}sa${sa}isa${isa}.assoc-part${i}of8"
    #echo $cmd
    #ln -sf \
    #  /mnt2T/data/live*al-minDeg${deg}WithTsAttr*npa${npa}*assoc*part${i}of8 \
    #  /mnt2T/data/liveJournal-augOpts-minDeg${deg}-npa${npa}sa${sa}isa${isa}.assoc-part${i}of8
      #/mnt2T/data/*al-npa${npa}*assoc*part${i}of8 \
  done

  cmd="./core/bin/graph-encoder \
    1 \
    $sa \
    $isa \
    $npa \
    /mnt2T/twitter/twitter2010-npa${npa}sa${sa}isa${isa}.assoc-part*of8"
  echo $cmd
  ./core/bin/graph-encoder \
    1 \
    $sa \
    $isa \
    $npa \
    /mnt2T/twitter/twitter2010-npa${npa}sa${sa}isa${isa}.assoc-part*of8
    #/mnt2T/data/liveJournal-augOpts-minDeg${deg}-npa${npa}sa${sa}isa${isa}.assoc-part*of8
    #/mnt2T/data/*al-augOpts-npa${npa}*assoc*of8
    #/mnt2T/data/live*${deg}WithTsAttr*npa${npa}*assoc*part*of8
}


deg=""
#for deg in 30 45 60; do
#  sa=4; isa=16; npa=16; encode
#  echo "L2 done"
#  sa=8; isa=64; npa=64; encode
#  echo "L1 done"
  sa=32; isa=64; npa=128; encode
  echo "L0 done"
#done
