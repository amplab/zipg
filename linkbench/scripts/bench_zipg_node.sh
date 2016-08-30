#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

node_id=$1
shift
servername=$1
shift
dataset=$1
shift
num_threads=$1
shift

if [ "$dataset" = "small" ]; then
  numnodes=32290001
elif [ "$dataset" = "medium" ]; then
  numnodes=403625000
elif [ "$dataset" = "large" ]; then
  numnodes=1026822000
else
  echo "Invalid dataset $dataset"
  exit -1
fi

QOPTS=""
if [ "$#" = "0" ]; then
  query_type="fb"
elif [ "$#" != "10" ]; then
  echo "Must provide exactly 10 query opts: provided $#"
  exit 0
else
  for opt in $@; do
    perc=$(echo $opt | sed 's/\-D\(.*\)=\(.*\)/\2/')
    qtyp=$(echo $opt | sed 's/\-D\(.*\)=\(.*\)/\1/')
    if [ "$perc" = "100" ]; then
      query_type=$qtyp
    fi
    QOPTS="$QOPTS $opt"
  done
fi

node_max_id=$(($numnodes + $node_id * 10000000 + 1))

echo "Executing benchmark on node $node_id with $num_threads threads for the $dataset dataset [node_max_id=$node_max_id]"
cmd="$sbin/../bin/linkbench -c $sbin/../config/LinkConfigSuccinct.properties -r -L $sbin/../zipg.t${num_threads}.q${query_type}.log -Drequesters=${num_threads} -Dhostname=$servername -Dnodeidoffset=$node_max_id $QOPTS"
echo $cmd
$cmd
