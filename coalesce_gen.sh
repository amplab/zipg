#!/bin/bash
set -e
make -j partitioned-graph-formatter

dataset=orkut-40attr16each
dataset="uk-2007-05"

if [[ "$dataset" == "uk-2007-05" ]]; then
  output_file_prefix="/vol0/uk-2007-05-40attr16each-npa128sa32isa64.assoc"
  num_shards=16
  attr_file=/vol0/data_0
  edge_attr_size=128
  inner_delim='	' # tab
  end_delim='^M'
  input_edgelists=(/vol0/uk-2007-05/part-*)
elif [[ "$dataset" == "orkut-40attr16each" ]]; then
  output_file_prefix=/vol0/orkut-40attr16each-npa128sa32isa64.assoc
  num_shards=8
  attr_file=/vol0/data_0
  edge_attr_size=128
  inner_delim='	' # tab
  end_delim='^M'
  input_edgelists=(/vol0/com-orkut.ungraph.txt)
elif [[ "$dataset" == "twitter2010-40attr16each" ]]; then
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
