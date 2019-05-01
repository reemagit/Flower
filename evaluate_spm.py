from __future__ import division
import networkx as nx
import utils
import argparse
from flower import calculate_spm, save_spm
import numpy as np


parser = argparse.ArgumentParser(description='Evaluate Shortest Path Metrics (SPM)')
parser.add_argument('network_file', type=str, help='Input network filepath with edgelist format (every line is e.g. \"node_1 node_2\")')
parser.add_argument('output', type=str, default=None, help='Output SPM file')
parser.add_argument('--max_path_length', type=int, help='Maximum shortest path length', default=100)
parser.add_argument('--int_nodeid', action='store_true', help='Cast the node IDs to integer')
parser.add_argument('--dry','--dry_run', action='store_true', help='Dry run')
args = parser.parse_args()


print "Loading graph..."

nodetype = int if args.int_nodeid else str
G = nx.read_edgelist(args.network_file, nodetype=nodetype)

nodelist = G.nodes()

A = nx.adjacency_matrix(G, nodelist=nodelist)

print "Number of nodes: ", len(G)

print "Completed."

print "Evaluating Shortest Path Metrics..."
num_paths, path_lengths, deglist, curr_iter = calculate_spm(A, args.max_path_length)
print "Completed."

if not args.dry:
    print "Writing..."
    save_spm(args.output, num_paths=num_paths, path_lengths=path_lengths, nodelist=nodelist, deglist=deglist, curr_iter=curr_iter)
    print "Completed."
