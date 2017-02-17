num_shards=${1:-"30"}
pad_width=${#num_shards}
for dataset in "500000" "1000000" "2000000" "4000000" "8000000"; do
  mkdir -p "$HOME/rpq/succinct/raw/$dataset/"
  echo "Processing dataset $dataset"
  ../core/bin/graph-partitioner -n $num_shards -t 0 $HOME/rpq/raw/$dataset/$dataset.node $HOME/rpq/raw/$dataset/$dataset.assoc
  ../core/bin/graph-partitioner -n $num_shards -t 1 $HOME/rpq/raw/$dataset/$dataset.node $HOME/rpq/raw/$dataset/$dataset.assoc
  for i in `seq 0 1 $((num_shards - 1))`; do
    p=$(printf "%0*d" ${pad_width} ${i})
    echo "Processing part${p}of${num_shards}"
    mkdir -p "$HOME/rpq/succinct/raw/$dataset"
    ../core/bin/linkbench-formatter "$HOME/rpq/raw/$dataset/$dataset" "$HOME/rpq/succinct/raw/$dataset/$dataset" "-part${p}of${num_shards}"&
  done
  wait
done
