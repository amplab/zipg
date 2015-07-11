#!/usr/bin/env bash

# NB: somehow if the whole name is specified, doesn't work on EC2 Linux
pids="`pgrep graph_query_ser`"

for pid in $pids
do
	echo "Killing pid $pid..."
	kill -9 $pid
done
