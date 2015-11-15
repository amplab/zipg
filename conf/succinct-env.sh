# NOTE: #shards should be a multiple of num_hosts for now.
export ENABLE_MULTI_STORE=F # T for enabled, anything else disabled
export TOTAL_NUM_SHARDS=8 
export num_hosts="$(cat "$(dirname "${BASH_SOURCE:-$0}")"/../conf/hosts | sed "s/#.*$//;/^$/d" | wc -l)"
# NOTE: change this manually to "shards per SuccinctStore server", if multistore
export SHARDS_PER_SERVER=$(($TOTAL_NUM_SHARDS / $num_hosts)) 

export NUM_SUFFIXSTORE_PARTS=1
export NUM_LOGSTORE_PARTS=3

currDir=$(cd $(dirname $0); pwd)
export LD_LIBRARY_PATH=${currDir}/external/succinct-cpp/lib:${LD_LIBRARY_PATH}

#export SHARDS_BATCH_SLEEP=69000 # seconds
#export NUM_SHARDS_BATCH=4
