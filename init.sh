#!/bin/bash
set -e
yum install -y cmake
git submodule update --init

yes | cp ~/spark-ec2/slaves conf/hosts >/dev/null

currDir=$(cd $(dirname $0); pwd)
. "${currDir}/sbin/succinct-config.sh"
. "${currDir}/sbin/load-succinct-env.sh"

echo "Num shards: ${TOTAL_NUM_SHARDS}; Num hosts: ${num_hosts}"

#${currDir}/sbin/hosts.sh 

sbin/copy-dir --delete ./
sbin/copy-dir --delete ~/.bashrc
sbin/copy-dir --delete ~/.gitconfig
