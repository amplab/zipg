setup=$1
dataset=$2
part=$3
construct=~/succinct-graph/core/bin/graphconstruct
s3cmd=~/s3cmd-master/s3cmd

split_path=/mnt2/$dataset/$setup
s3path=s3://succinct-datasets/linkbench/raw/$dataset/$setup

if [ "$setup" = "singlemachine" ]; then
  num_shards=30
elif [ "$setup" = "distributed" ]; then
  num_shards=40
else
  echo "[setup] must be one of 'distributed' or 'singlemachine'; currently set to [$setup]"
  exit -1
fi

printf -v j "%02d" $part
node_file=${dataset}.node-part${j}of${num_shards}WithPtrs
edge_file=${dataset}.edge_table-part${j}of${num_shards}

# Download part files
echo "Downloading $node_file, $edge_file and $delete_file..."
mkdir -p $split_path
$s3cmd get $s3path/$node_file $split_path/$node_file
$s3cmd get $s3path/$edge_file $split_path/$edge_file
echo "Finished downloading $node_file, $edge_file and $delete_file"

# Compress part files
echo "Compressing shard $part"
$construct 32 64 128 $split_path/$node_file $split_path/$edge_file
echo "Finished compressing shard $part"

# Upload part files
succinct_s3path=s3://succinct-datasets/linkbench/succinct/$dataset/$setup/
$s3cmd put --no-check-md5 --recursive $split_path/$node_file.succinct $succinct_s3path
$s3cmd put --no-check-md5 --recursive $split_path/$edge_file.succinct $succinct_s3path
rm -r $split_path
