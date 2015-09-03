# NOTE: #shards should be a multiple of num_hosts for now.
export TOTAL_NUM_SHARDS=8

# hacky
export num_hosts="$(cat "$(dirname "${BASH_SOURCE:-$0}")"/../conf/hosts | sed "s/#.*$//;/^$/d" | wc -l)"

export SHARDS_PER_SERVER=$(($TOTAL_NUM_SHARDS / $num_hosts))

#export SHARDS_BATCH_SLEEP=69000 # seconds
#export NUM_SHARDS_BATCH=4
