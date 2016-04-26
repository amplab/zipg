#!/usr/bin/env bash
ids="`pgrep graph_query_agg`"

for pid in $pids
do
	echo "Killing pid $pid..."
	kill -9 $pid
done