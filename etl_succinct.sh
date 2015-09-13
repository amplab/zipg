#!/bin/bash
set -e

assocShardDir=... # absolute path
numShards=...
encodeType=0 # 0 for edge table

#### Steps
#### 0. PREREQ: successfully compiles on this master machine
#### 1. change coalesce_gen.sh to adjust paths/settings
#### 2. change `assocShardDir` to match the one in coalesce_gen.sh
#### 3. change the TODO below

#################### 
~/spark-ec2/copy-dir --delete ./

#################### 
bash ./coalesce_gen.sh
echo "Coalescing generation done"

#################### 
for i in $(seq 1 1 $numShards); do
  hostname=$(sed -n "${i}{p;q;}" ~/spark-ec2/slaves | tr '\n' ' ')
  p=$(printf "%0*d" 2 $i) # TODO

  rsync -avr --progress \
    ${assocShardDir}/dataset.assoc-part${p}of${numShards} \  # TODO: change
    root@${hostname}:${assocShardDir} &

  cat >/root/succinct-graph/etl_tmp.sh <<EOL
#!/bin/bash
set -e
bash /root/succinct-graph/encoder.sh ${encodeType} ${assocShardDir}/dataset.assoc-part${p}of${numShards}
EOL
done
wait
echo "Copied corresponding shard files from ${assocShardDir} to workers"

#################### 
~/spark/sbin/slaves.sh \
  bash /root/succinct-graph/etl_tmp.sh
