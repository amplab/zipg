#!/usr/bin/env bash

sbin=`dirname "$0"`
sbin=`cd "$sbin"; pwd`

# Stop master
echo "Stopping master..."
bash $sbin/stop-master.sh

# Stop workers
echo "Stopping workers..."
bash $sbin/stop-workers.sh
