#!/bin/bash
for nodes in 1000 
do
	freq=1
	while [ $freq -lt $nodes ]
	do
		echo creating succinct graph: ${nodes}_${freq}
                ../bin/create nodes ${nodes} ${freq}
		../bin/create succinct ${nodes}_${freq}.node ${nodes}.edge
		freq=$((freq * 10))
	done
done
