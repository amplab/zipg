#!/usr/bin/env python

import sys
from networkx import random_regular_graph

if len(sys.argv) < 3:
    print "Usage: ./generate_graph.py [num_nodes] [node_degree]"
    sys.exit(1)

num_nodes = int(sys.argv[1])
node_degree = int(sys.argv[2])

graph = random_regular_graph(node_degree, num_nodes)
edge_list = []
for edge in graph.edges():
    edge_list.append('{0} {1}\n'.format(edge[0], edge[1]))

with open(str(num_nodes) + '.txt', 'w') as f:
    f.writelines(edge_list)

