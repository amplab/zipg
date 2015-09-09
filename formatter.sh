#!/bin/bash
set -e
make bench

dataset=higgs-40attr16each
dataset=liveJournal-40attr16each
dataset=twitter2010-40attr16each

if [[ "$dataset" == "twitter2010-40attr16each" ]]; then
  edgelist=TODO
  num_nodes=1000000
  num_nodes=41652230
  num_node_attr=1
  num_node_attr=40
  zipf_corpus_size=1000
  node_attr_size_each=16
  inner_delim='	' # tab
  end_delim='^M'
  assoc_out_file=TODO
  node_out_file=/mnt2/twitter2010-${num_node_attr}attr${node_attr_size_each}each-tpch.node
elif [[ "$dataset" == "liveJournal-40attr16each" ]]; then
  edgelist=/mnt2/soc-LiveJournal1.txt
  num_nodes=4847571
  num_node_attr=40
  node_attr_freq=1000
  node_attr_size_each=16
  inner_delim='	'
  end_delim=''
  assoc_out_file=liveJournal.assoc
  node_out_file=liveJournal-${num_node_attr}attr${node_attr_size_each}each-tpch.node
elif [[ "$dataset" == "higgs-40attr16each" ]]; then
  edgelist=./data/higgs-social_network.assoc
  num_nodes=456627
  num_node_attr=40
  node_attr_freq=1000
  node_attr_size_each=16
  inner_delim='	'
  end_delim=''
  assoc_out_file=NOT_NEEDED_FOR_NOW
  node_out_file=${dataset}-tpch.node
else 
  exit 1
fi

i=0
for num in `seq 0 1 0`; do
  padded=$(printf "%0*d" 5 ${num})
  f="part-${padded}"
  assoc_out_file=${f}.assoc
#  if [[ -f "/mnt2T/twitter/$assoc_out_file" ]]; then
#    echo /mnt2T/twitter/$assoc_out_file exists, skipping
#    continue
#  fi
  echo i=$i, working on file "${f}"

  ./benchmark/bin/create \
    format-input \
    /mnt2/twitter/${f} \
    data/data_0 \
    /mnt2T/twitter/${assoc_out_file} \
    data/${node_out_file} \
    $num_nodes \
    $num_node_attr \
    $zipf_corpus_size \
    $node_attr_size_each \
    "${inner_delim}" \
    "${end_delim}" &
  i=$((i + 1))
  if [[ ( "$i" == 30 ) ]]; then
    echo "waiting for batch"
    wait
    i=0
  fi
done
wait

# check sizes
#for num in `seq 0 1 194`; do
#  padded=$(printf "%0*d" 5 ${num})
#  f="part-${padded}"
#  assoc_out_file=${f}.assoc
#
#  assocLines=$(wc -l /mnt2T/twitter/${assoc_out_file} | cut -d' ' -f1)
#  inputLines=$(wc -l /mnt2/twitter/${f} | cut -d' ' -f1)
#  
#  if [[ ! ( $assocLines == $inputLines ) ]]; then
#    echo for file $f, lines not equal: $assocLines, $inputLines
#  fi
#
#done
#echo "Sizes check done!"
