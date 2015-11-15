#!/bin/bash
set -e
make -j bench

dataset=twitter2010-40attr16each

if [[ "$dataset" == "twitter2010-40attr16each" ]]; then
  store_out="${dataset}"
  num_nodes=41652230
  num_atypes=5
  assoc_set_file=/vol0/twitter2010-assoc_lists.set
  attr_file=/vol0/data_0
  bytes_per_attr=128
  min_time=1439721981221
  max_time=1441905687237
else 
  exit 1
fi

# Suffix Store
store_out_file="${store_out}.suffixstore"
num_edges_to_add=1
./benchmark/bin/create make-store \
  ${store_out_file} \
  ${num_edges_to_add} \
  ${num_nodes} \
  ${num_atypes} \
  ${assoc_set_file} \
  ${attr_file} \
  ${bytes_per_attr} \
  ${min_time} \
  ${max_time} \
  0 # for suffix store

# Log Store
store_out_file="${store_out}.logstore"
num_edges_to_add=1
./benchmark/bin/create make-store \
  ${store_out_file} \
  ${num_edges_to_add} \
  ${num_nodes} \
  ${num_atypes} \
  ${assoc_set_file} \
  ${attr_file} \
  ${bytes_per_attr} \
  ${min_time} \
  ${max_time} \
  1 # for log store
