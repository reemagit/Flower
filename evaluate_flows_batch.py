from __future__ import division
import utils
import flower
import networkx as nx
import argparse
import pandas as pd
import numpy as np

parser = argparse.ArgumentParser(description='Evaluate flow centrality values for a batch of many source and target node sets')
parser.add_argument('network_file', type=str, help='Input network filepath with edgelist format (every line is e.g. \"node_1 node_2\")')
parser.add_argument('srcdestmodules_file', type=str, help='List of sets of source and destination nodes, where every line has format \"node1_1 node1_2 node1_3|node2_1 node2_2 node2_3\", with node1_x are source nodes and node2_y are target nodes')
parser.add_argument('spm_file', type=str, help='Shortest Path Metrics file in h5 format')
parser.add_argument('output_file', type=str, help='Flow values output filepath (dataframe where i,j-th element is the FC of the i-th node for the j-th pair of source|destination node sets (j-th row)')
parser.add_argument('--n_cores', type=int, default=1, help='Number of parallel processes')
parser.add_argument('--int_nodeid', action='store_true', help='Cast the node IDs to integer')
parser.add_argument('--dry','--dry_run', action='store_true', help='Dry run')
args = parser.parse_args()

print "Importing network..."

nodetype = int if args.int_nodeid else str
ppi = nx.read_edgelist(args.network_file, nodetype=nodetype)

print "Loading SPM data..."

spm_data = flower.load_spm(args.spm_file)

nodelist = spm_data['nodelist']

with open(args.srcdestmodules_file, 'r') as f:
    srcrows,destrows = zip(*map(lambda x: x.rstrip().split('|'),f.readlines())) # this line is tricky but it just splits the rows in srcmodule and destmodule

if args.int_nodeid:
	srcmodules = map(lambda x: map(int,x.split(' ')), srcrows)
	destmodules = map(lambda x: map(int,x.split(' ')), destrows)
else:
	srcmodules = map(lambda x: x.split(' '), srcrows)
	destmodules = map(lambda x: x.split(' '), destrows)

gm = utils.GIDMapper(nodelist=nodelist)

nodes_dest_list = map(gm.gid2id, destmodules)
nodes_src_list = map(gm.gid2id, srcmodules)

print "Evaluating flows..."

def wrap(pair):
    nodes_dest, nodes_src = pair[0],pair[1]
    return flower.eval_flow_centrality(nodes_dest, nodes_src, spm=spm_data, output_mode='FLOWS_NORM', progressbar=False)

if args.n_cores > 1:
	flow_values_all = utils.parallel_process(wrap, zip(nodes_dest_list, nodes_src_list),)
else:
	flow_values_all = map(wrap, zip(nodes_dest_list, nodes_src_list),)
flow_values_all = np.asarray(flow_values_all)
flow_values_df = pd.DataFrame(flow_values_all, index=spm_data['nodelist'], columns=['nodeset_pair_' + str(i) for i in range(len(srcmodules))])

print "Completed."

if not args.dry:
    print "Writing to file..."
    flow_values_df.to_csv(args.output_file, sep='\t')
    print "Completed."