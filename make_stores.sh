#!/bin/bash
set -e
make -j bench

dataset=twitter2010-40attr16each

num_suff_stores=1
num_edges_per_suff_store=1
num_log_stores=1
num_edges_per_log_store=1

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

# SuffixStores

padWidth=${#num_suff_stores}

for i in $(seq 1 1 ${num_suff_stores}); do
  p=$(printf "%0*d" ${padWidth} ${i})
  store_out_file="${store_out}.suffixstore-part${p}of${num_suff_stores}"

  ./benchmark/bin/create make-store \
    ${store_out_file} \
    ${num_edges_per_suff_store} \
    ${num_nodes} \
    ${num_atypes} \
    ${assoc_set_file} \
    ${attr_file} \
    ${bytes_per_attr} \
    ${min_time} \
    ${max_time} \
    0 # for suffix store
done

# LogStores

padWidth=${#num_suff_stores}

for i in $(seq 1 1 ${num_log_stores}); do
  p=$(printf "%0*d" ${padWidth} ${i})
  store_out_file="${store_out}.logstore-part${p}of${num_log_stores}"

  ./benchmark/bin/create make-store \
    ${store_out_file} \
    ${num_edges_per_log_store} \
    ${num_nodes} \
    ${num_atypes} \
    ${assoc_set_file} \
    ${attr_file} \
    ${bytes_per_attr} \
    ${min_time} \
    ${max_time} \
    1 # for log store
done
