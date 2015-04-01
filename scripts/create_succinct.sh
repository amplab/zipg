#!/bin/bash
for nodes in 2000000 3000000 4000000 #10000 100000 1000000 2000000 3000000 4000000 5000000
do
	freq=1
	while [ $freq -lt $nodes ]
	do
		echo creating succinct graph: ${nodes}_${freq}
                ./succinct-graph/bin/create nodes ${nodes} ${freq}
		./succinct-graph/bin/create succinct ${nodes}_${freq}.node ${nodes}.edge
		freq=$((freq * 10))
	done
done
