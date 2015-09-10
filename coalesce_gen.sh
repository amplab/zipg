#!/bin/bash
set -e
make -j partitioned-graph-formatter

dataset=twitter2010-40attr16each

if [[ "$dataset" == "twitter2010-40attr16each" ]]; then
  output_file_prefix=/mnt2T/twitter2010-npa128sa32isa64.assoc
  num_shards=16
  attr_file=/mnt/succinct-graph/data/data_0
  edge_attr_size=128
  inner_delim='	' # tab
  end_delim='^M'
  input_edgelists=(/mnt2/twitter/part-*)
else
  exit 1
fi

./core/bin/partitioned-graph-formatter \
  ${output_file_prefix} \
  ${num_shards} \
  ${attr_file} \
  ${edge_attr_size} \
  "${inner_delim}" \
  "${end_delim}" \
  ${input_edgelists[*]}
