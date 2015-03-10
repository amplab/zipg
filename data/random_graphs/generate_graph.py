#!/usr/bin/env python

import sys
import numpy as np
from networkx import random_regular_graph

if len(sys.argv) < 3:
    print "Usage: ./generate_graph.py [num_nodes] [node_degree]"
    sys.exit(1)

num_nodes = int(sys.argv[1])
node_degree = int(sys.argv[2])

edge_list = []
if num_nodes <= 1000000:
    graph = random_regular_graph(node_degree, num_nodes)
    for edge in graph.edges():
        edge_list.append('{0} {1}\n'.format(edge[0], edge[1]))
else:
    n = num_nodes
    node_degree_left = np.ones(num_nodes) * node_degree
    nodes_remaining = np.array(range(n))
    while n > 0:
        from_node_idx = np.random.randint(0, n)
        from_node = nodes_remaining[from_node_idx]
        degree = node_degree_left[from_node]
        nodes_remaining[from_node_idx] = nodes_remaining[n-1]
        n -= 1
        to_node_idxs = np.random.randint(0, n, degree)
        for to_node_idx in to_node_idxs:
            to_node = nodes_remaining[to_node_idx]
            node_degree_left[to_node] -= 1
            edge_list.append('{0} {1}\n'.format(from_node, to_node))
        for to_node_idx in to_node_idxs:
            to_node = nodes_remaining[to_node_idx]
            if node_degree_left[to_node] == 0:
                nodes_remaining[to_node_idx] = nodes_remaining[n-1]
                n -= 1

with open(str(num_nodes) + '.txt', 'w') as f:
    f.writelines(edge_list)

