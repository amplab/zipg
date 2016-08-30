#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

dataset=$1

echo "Copying benchmark directory"
$sbin/copy-dir $sbin/../

for num_threads in 1; do
  $sbin/bench_single_client.sh $dataset $num_threads
done
