#!/bin/bash
set -e
make bench

dataset=higgs-40attr16each

if [[ "$dataset" == "liveJournal-40attr16each" ]]; then
  edgelist=/mnt/soc-LiveJournal1.txt
  num_nodes=4847571
  num_node_attr=40
  node_attr_freq=1000
  node_attr_size_each=16
  inner_delim='	'
  end_delim=''
elif [[ "$dataset" == "higgs-40attr16each" ]]; then
  edgelist=./data/higgs-social_network.assoc
  num_nodes=456627
  num_node_attr=40
  node_attr_freq=1000
  node_attr_size_each=16
  inner_delim='	'
  end_delim=''
else 
  exit 1
fi

./bin/create \
  format-input \
  $edgelist \
  data/data_0 \
  data/liveJournal.assoc \
  data/liveJournal-${num_node_attr}attr${node_attr_size_each}each-tpch.node \
  $num_nodes \
  $num_node_attr \
  $node_attr_freq \
  $node_attr_size_each \
  "${inner_delim}" \
  "${end_delim}"
