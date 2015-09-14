#!/bin/bash
set -e

dataset="uk-2007-05-40attr16each"
dataset="orkut-40attr16each"

assocShardDir=/vol0/
numShards=8
numDigitsOfNumShards=1
encodeType=0 # 0 for edge table

num_nodes=41652230 # TODO
num_node_attr=40
zipf_corpus_size=4000000 # TODO: ~1/10 of #node?
node_attr_size_each=16
attr_file=/vol0/data_0
node_out_file=/vol0/${dataset}-tpch-npa128sa32isa64.node

#### Steps
#### 0. PREREQ: successfully compiles on this master machine
#### 1. change coalesce_gen.sh to adjust paths/settings
#### 2. change `assocShardDir` to match the one in coalesce_gen.sh
#### 3. change all TODO settings above

#################### 
~/spark/sbin/slaves.sh yum install -y make glibc-devel gcc
~/spark-ec2/copy-dir ./

#################### 
if [ "$encodeType" == "0" ]; then
  echo "Encoding edge table shards"
  #bash ./coalesce_gen.sh
  #echo "Coalescing generation done"
elif [ "$encodeType" == "1" ]; then
  echo "Encoding node table shards"
  ./benchmark/bin/create create-nodeTable \
    ${attr_file} ${node_out_file} ${num_nodes} ${num_node_attr} \
    ${zipf_corpus_size} ${node_attr_size_each}
  ./core/bin/graph-partitioner \
    -n ${numShards} -t 1 ${node_out_file} DUMMY
  echo "Node table creation done"
else
  echo "Unknown encodeType: ${encodeType}, skipping"
fi

#################### 
masterHostName=$(curl http://169.254.169.254/latest/meta-data/public-hostname)
echo "My own hostname: ${masterHostName}"

for i in $(seq 0 1 $numShards); do
  if [ "$i" == "$numShards" ]; then
    continue
  fi

  j=$((i + 1))
  hostname=$(sed -n "${j}{p;q;}" ~/spark-ec2/slaves | sed 's/\n//g')
  p=$(printf "%0*d" $numDigitsOfNumShards $i)
  
  targetFile="${assocShardDir}/${dataset}-npa128sa32isa64.assoc-part${p}of${numShards}"
  encoded=$(echo -n "${targetFile}.succinct" | sed 's/\(.*\)assoc\(.*\)/\1edge_table\2/')

  if [ "$encodeType" == "1" ]; then
    targetFile="${node_out_file}-part${p}of${numShards}"
    encoded="${targetFile}WithPtrs.succinct"
  fi

  rsync -avr --progress ${targetFile} root@${hostname}:${assocShardDir} &

  cat >/vol0/succinct-graph/etl_tmp.sh <<EOL
#!/bin/bash
set -e
bash /vol0/succinct-graph/encoder.sh ${encodeType} ${targetFile}
rsync -e "ssh -o StrictHostKeyChecking=no" -avr --progress \
  ${encoded} root@${masterHostName}:/vol0/
EOL

  rsync /vol0/succinct-graph/etl_tmp.sh \
    root@${hostname}:/vol0/succinct-graph/
done
wait
echo "Copied corresponding shard files from ${assocShardDir} to workers"

#################### 
~/spark/sbin/slaves.sh \
  bash /vol0/succinct-graph/etl_tmp.sh
rm -rf /vol0/succinct-graph/etl_tmp.sh
