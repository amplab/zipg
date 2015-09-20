# NOTE: #shards should be a multiple of num_hosts for now.
export TOTAL_NUM_SHARDS=8

# hacky
export num_hosts="$(cat "$(dirname "${BASH_SOURCE:-$0}")"/../conf/hosts | sed "s/#.*$//;/^$/d" | wc -l)"

export SHARDS_PER_SERVER=$(($TOTAL_NUM_SHARDS / $num_hosts))

currDir=$(cd $(dirname $0); pwd)
export LD_LIBRARY_PATH=${currDir}/external/succinct-cpp/lib:${LD_LIBRARY_PATH}

#export SHARDS_BATCH_SLEEP=69000 # seconds
#export NUM_SHARDS_BATCH=4
