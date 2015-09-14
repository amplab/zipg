#!/bin/bash
set -e

assocShardDir=/vol0/
numShards=8
encodeType=0 # 0 for edge table

#### Steps
#### 0. PREREQ: successfully compiles on this master machine
#### 1. change coalesce_gen.sh to adjust paths/settings
#### 2. change `assocShardDir` to match the one in coalesce_gen.sh
#### 3. change the TODO below

#################### 
~/spark/sbin/slaves.sh yum install -y make glibc-devel gcc
~/spark-ec2/copy-dir ./

#################### 
#bash ./coalesce_gen.sh
#echo "Coalescing generation done"

#################### 
masterHostName=$(curl http://169.254.169.254/latest/meta-data/public-hostname)
echo "My own hostname: ${masterHostName}"

for i in $(seq 0 1 $numShards); do
  if [ "$i" == "$numShards" ]; then
    continue
  fi

  j=$((i + 1))
  hostname=$(sed -n "${j}{p;q;}" ~/spark-ec2/slaves | sed 's/\n//g')

  p=$(printf "%0*d" 1 $i) # TODO
  
  # TODO: change
  targetFile="${assocShardDir}/uk-2007-05-40attr16each-npa128sa32isa64.assoc-part${p}of${numShards}"
  targetFile="${assocShardDir}/orkut-40attr16each-npa128sa32isa64.assoc-part${p}of${numShards}"
  encoded=$(echo -n "${targetFile}.succinct" | sed 's/\(.*\)assoc\(.*\)/\1edge_table\2/')
  rsync -avr --progress ${targetFile} root@${hostname}:${assocShardDir} &

  cat >/vol0/succinct-graph/etl_tmp.sh <<EOL
#!/bin/bash
set -e
#bash /vol0/succinct-graph/encoder.sh ${encodeType} ${targetFile}
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
