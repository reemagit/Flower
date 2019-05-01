from __future__ import division
import utils
import flower
import networkx as nx
import argparse
import pandas as pd
import numpy as np
import os

parser = argparse.ArgumentParser(description='Evaluate flow centrality values')
parser.add_argument('network_file', type=str, help='Input network filepath with edgelist format (every line is e.g. \"node_1 node_2\")')
parser.add_argument('srcmodule_file', type=str, help='Source nodes IDs, one per line')
parser.add_argument('destmodule_file', type=str, help='Destination nodes IDs, one per line')
parser.add_argument('spm_file', type=str, help='Shortest Path Metrics file')
parser.add_argument('output_file', type=str, help='Flow values output filepath')
parser.add_argument('--int_nodeid', action='store_true', help='Cast the node IDs to integer')
parser.add_argument('--dry','--dry_run', action='store_true', help='Dry run')
args = parser.parse_args()

print "Importing network..."

nodetype = int if args.int_nodeid else str
ppi = nx.read_edgelist(args.network_file, nodetype=nodetype)

print "Loading SPM data..."

spm_data = flower.load_spm(args.spm_file)

with open(args.srcmodule_file, 'r') as f:
	srcnodes = [row.rstrip() for row in f.readlines()]
with open(args.destmodule_file, 'r') as f:
	destnodes = [row.rstrip() for row in f.readlines()]
if args.int_nodeid:
	srcnodes = map(int, srcnodes)
	destnodes = map(int, destnodes)

gm = utils.GIDMapper(nodelist=spm_data['nodelist'])
nodes_dest = gm.gid2id(destnodes)
nodes_src = gm.gid2id(srcnodes)

print "Evaluating flows..."

flows = flower.eval_flow_centrality(nodes_dest, nodes_src, spm=spm_data)

data = pd.DataFrame(zip(spm_data['nodelist'], flows[:, 0], flows[:, 1], flows[:,2].astype(int)), columns=['NodeID', 'Flow_value', 'Flow_value_unnorm','Num_paths'])
data.NodeID = data.NodeID.astype(int)
data.set_index('NodeID', inplace=True)

print "Completed."

print data.iloc[:10]

if not args.dry:
    print "Writing to file..."
    data.to_csv(args.output_file, sep='\t')
    print "Completed."