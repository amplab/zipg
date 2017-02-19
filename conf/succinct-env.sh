# NOTE: #shards should be a multiple of num_hosts for now.
export ENABLE_MULTI_STORE=T # T for enabled, anything else disabled
export TOTAL_NUM_SHARDS=30
export num_hosts=1
# NOTE: change this manually to "shards per SuccinctStore server", if multistore
export SHARDS_PER_SERVER=30

export NUM_SUFFIXSTORE_PARTS=0
export NUM_LOGSTORE_PARTS=0

currDir=$(cd $(dirname $0); pwd)
export LD_LIBRARY_PATH=${currDir}/external/succinct-cpp/lib:${LD_LIBRARY_PATH}

#export SHARDS_BATCH_SLEEP=69000 # seconds
#export NUM_SHARDS_BATCH=4
