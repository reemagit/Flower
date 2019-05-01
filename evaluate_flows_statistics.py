from __future__ import division
import networkx as nx
import utils
import flower
import argparse
import pandas as pd
import numpy as np

parser = argparse.ArgumentParser(description='Evaluate flow centrality statistics (average and standard deviation) for set of samples')
parser.add_argument('network_file', type=str, help='Input network filepath with edgelist format (every line is e.g. \"node_1 node_2\")')
parser.add_argument('srcdestmodules_file', type=str, help='List of sets of source and destination nodes, where every line has format \"node1_1 node1_2 node1_3|node2_1 node2_2 node2_3\", with node1_x are source nodes and node2_y are target nodes')
parser.add_argument('spm_file', type=str, help='Shortest Path Metrics file')
parser.add_argument('output_file', type=str, help='Flow values output filepath')
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

srcmodules = map(lambda x: map(nodetype,x.split(' ')), srcrows)
destmodules = map(lambda x: map(nodetype,x.split(' ')), destrows)

gm = utils.GIDMapper(nodelist=nodelist)

nodes_dest_list = map(gm.gid2id, destmodules)
nodes_src_list = map(gm.gid2id, srcmodules)

print "Evaluating flows..."

def wrap(pair):
    nodes_dest, nodes_src = pair[0],pair[1]
    return flower.eval_flow_centrality(nodes_dest, nodes_src, spm=spm_data, progressbar=False)

if args.n_cores > 1:
	flow_values_all = utils.parallel_process(wrap, zip(nodes_dest_list, nodes_src_list),)
else:
	flow_values_all = map(wrap, zip(nodes_dest_list, nodes_src_list),)
flow_values_all = np.asarray(flow_values_all)

flow_values = flow_values_all[:,:,0].T

flow_values_mean = flow_values.mean(axis=1)
flow_values_std = flow_values.std(axis=1)

data = pd.DataFrame(zip(nodelist, flow_values_mean, flow_values_std), columns=['NodeID', 'Flow_mean', 'Flow_std'])
#data.NodeID = data.NodeID.astype(int)
data.set_index('NodeID', inplace=True)

print "Completed."

print data.iloc[:10]

if not args.dry:
    print "Writing to file..."
    data.to_csv(args.output_file, sep='\t')
    print "Completed."