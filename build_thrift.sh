#!/bin/bash
set -e

currDir=$(cd $(dirname $0); pwd)
pushd ${currDir}/external/succinct-cpp/ >/dev/null
sudo bash ec2/install_thrift.sh
popd
