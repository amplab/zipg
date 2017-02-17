#!/bin/bash
set -e

num_shards=${1:-"30"}
genFromEdgeList=true # if false, coalesce from assocs
attr_file=$HOME/rpq/empty
edge_attr_size=0
inner_delim='	' # tab
end_delim='^M'

currDir=$(cd $(dirname $0); pwd)
export LD_LIBRARY_PATH="${currDir}/../external/succinct-cpp/lib:/usr/local/lib:${LD_LIBRARY_PATH}"

echo $LD_LIBRARY_PATH

for dataset in "500000" "1000000" "2000000" "4000000" "8000000"; do
  mkdir -p "$HOME/rpq/succinct/$dataset"
  input_edgelists=($HOME/rpq/succinct/raw/$dataset/$dataset.assoc)
  output_file_prefix="$HOME/rpq/succinct/$dataset/$dataset.assoc"
  ../core/bin/partitioned-graph-formatter \
    ${genFromEdgeList} \
    ${output_file_prefix} \
    ${num_shards} \
    ${attr_file} \
    ${edge_attr_size} \
    "${inner_delim}" \
    "${end_delim}" \
    ${input_edgelists[*]}
done
