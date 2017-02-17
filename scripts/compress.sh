num_shards=${1:-"30"}

construct="$HOME/zipg/core/bin/graphconstruct"

for dataset in "500000" "1000000" "2000000" "4000000" "8000000"; do
  split_path="$HOME/rpq/succinct/raw/$dataset"
  for part in `seq 0 1 $((num_shards - 1))`; do
    printf -v j "%02d" $part
    node_file="${dataset}.node-part${j}of${num_shards}"
    edge_file="${dataset}.assoc-part${j}of${num_shards}"

    # Compress part files
    echo "Compressing shard $part"
    $construct 32 64 128 $split_path/$node_file $split_path/$edge_file &
    #echo "Finished compressing shard $part"
  done
  wait
  echo "Finished dataset $dataset"
done
