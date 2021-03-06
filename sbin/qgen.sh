#!/bin/bash
set -ex

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/succinct-config.sh"
. "$sbin/load-succinct-env.sh"

script_dir=$sbin/../scripts

npa=128; sa=32; isa=64 # L0, by default

if [ -f "$SUCCINCT_CONF_DIR/master" ]; then
  master=`cat "$SUCCINCT_CONF_DIR/master"`
else
  master="localhost"
fi

warmup_n=200000 # Per server
measure_n=1000000 # Per server

datasets=(
  #uk
  #twitter
  orkut
)
queries=(
	neighborAtype
	#node
	neighbor
	neighborNode
	assocRange
	assocGet
	assocCount
	assocTimeRange
	objGet
)

function timestamp() {
  date +"%D-%T"
}

function setup() {
  # By default disable strict host key checking
  if [ "$SUCCINCT_SSH_OPTS" = "" ]; then
    SUCCINCT_SSH_OPTS="-o StrictHostKeyChecking=no -i $SUCCINCT_CONF_DIR/cqlkeypair.pem"
  fi

  if [ "$dataset" = "twitter" ]; then
    node_file_raw=/mnt2/twitter2010-40attr16each-tpch.node
    edge_file_raw=/mnt2/twitter2010-npa128sa32isa64.assoc
		query_dir=/mnt2/twitterQueries
		num_nodes=41652230
  elif [ "$dataset" = "uk" ]; then
    node_file_raw=/mnt2/uk-2007-05-40attr16each-tpch.node
    edge_file_raw=/mnt2/uk-2007-05-40attr16each-npa128sa32isa64.assoc
    query_dir=/mnt2/ukQueries
		num_nodes=105896555
  elif [ "$dataset" = "orkut" ]; then
    node_file_raw=/mnt2/orkut-40attr16each-tpch-npa128sa32isa64.node
    edge_file_raw=/mnt2/orkut-40attr16each-npa128sa32isa64.assoc
    query_dir=/mnt2/orkutQueries
		num_nodes=3072627
  else
    echo "Must specify dataset."
    exit
  fi
	
  bash $sbin/hosts.sh mkdir -p $query_dir
	bash $sbin/hosts.sh rm $query_dir/*
  
	if [ "$master" = "localhost" ]; then
		bash $sbin/../scripts/setup_dist.sh $node_file_raw $edge_file_raw $sa $isa $npa
	else
  	ssh $SUCCINCT_SSH_OPTS "$master" "bash $sbin/../scripts/setup_dist.sh $node_file_raw $edge_file_raw $sa $isa $npa"
	fi
}

for dataset in "${datasets[@]}"; do
	setup
  for query in "${queries[@]}"; do
    echo "Generating queries for ($query, $dataset) in $query_dir, with $num_nodes nodes."
		bash $sbin/hosts.sh $query=T bash $script_dir/create_queries.sh $query_dir $num_nodes $warmup_n $measure_n $node_file_raw $edge_file_raw
	done
done
