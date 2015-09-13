#!/bin/bash
set -e

encodeType=$1
shard=$2

if [ "$encodeType" = "" ]; then
  encodeType=0 # 0 for edge table
  encodeType=1 # 1 for node table
fi

if [ "$shard" = "" ]; then
  shard=/mnt2T/twitter/twitter2010-npa${npa}sa${sa}isa${isa}.assoc-part*of8
fi

function encode() {
  ./core/bin/graph-encoder \
    1 \
    $sa \
    $isa \
    $npa \
    ${encodeType} \
    ${shard}
    #/mnt2T/data/liveJournal-augOpts-minDeg${deg}-npa${npa}sa${sa}isa${isa}.assoc-part*of8
    #/mnt2T/data/*al-augOpts-npa${npa}*assoc*of8
    #/mnt2T/data/live*${deg}WithTsAttr*npa${npa}*assoc*part*of8
}

# sa=4; isa=16; npa=16; encode
# echo "L2 done"
# sa=8; isa=64; npa=64; encode
# echo "L1 done"
sa=32; isa=64; npa=128; encode
echo "L0 done: ${shard}"
