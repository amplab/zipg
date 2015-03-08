#!/usr/bin/python

import sys, os

input_directory = 'unprocessed/'
output_directory = 'preprocessed/'
delimiter = ','
default_suffix = '.normalized'

def normalize(file_name, input_directory, output_directory, suffix=''):
    input_file = open(input_directory + file_name, 'r')
    output_file = open(output_directory + file_name + suffix, 'w')

    num_nodes = 0
    node_to_id = {}

    for line in input_file.readlines():
        from_node, to_node = line.split()
        if from_node not in node_to_id:
            node_to_id[from_node] = num_nodes
            num_nodes += 1
        if to_node not in node_to_id:
            node_to_id[to_node] = num_nodes
            num_nodes += 1

        from_id = node_to_id[from_node]
        to_id = node_to_id[to_node]
        output_file.write(
            '{0}{1}{2}\n'.format(from_id, delimiter, to_id)
        )
    print "Wrote to output file: " + output_directory + file_name + suffix

if len(sys.argv) == 1:
    input_files = [f for f in os.listdir(input_directory) if f.endswith(".txt")]
    output_files = [f for f in os.listdir(output_directory) if f.endswith(".txt")]
    for input_file in input_files:
        if input_file not in output_files:
            normalize(input_file, input_directory, output_directory)
else:
    for file_name in sys.argv[1:]:
        normalize(file_name, '', '', default_suffix)

