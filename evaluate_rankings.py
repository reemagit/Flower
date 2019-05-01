from __future__ import division
import utils
import flower
import networkx as nx
import argparse
import pandas as pd
import numpy as np
import os

parser = argparse.ArgumentParser(description='Evaluate flow centrality values')
parser.add_argument('flows_file', type=str, help='Input flow values filepath (generated with evaluate_flows.py)')
parser.add_argument('flows_stats_file', type=str, help='Input flow statistics filepath (generated with evaluate_flows_statistics.py)')
parser.add_argument('network_file', type=str, help='Input network filepath with edgelist format (every line is e.g. \"node_1 node_2\")')
parser.add_argument('srcmodule_file', type=str, help='Source nodes IDs, one per line')
parser.add_argument('destmodule_file', type=str, help='Destination nodes IDs, one per line')
parser.add_argument('spm_file', type=str, help='Shortest Path Metrics file')
parser.add_argument('output_file', type=str, help='Flow values output filepath')
parser.add_argument('--int_nodeid', action='store_true', help='Cast the node IDs to integer')
parser.add_argument('--dry','--dry_run', action='store_true', help='Dry run')
args = parser.parse_args()

nodetype = int if args.int_nodeid else str
ppi = nx.read_edgelist(args.network_file, nodetype=nodetype)

flows = pd.read_csv(args.flows_file,sep='\t',dtype={'NodeID':nodetype})
flows_stats = pd.read_csv(args.flows_stats_file,sep='\t',dtype={'NodeID':nodetype})

with open(args.srcmodule_file, 'r') as f:
	srcnodes = [row.rstrip() for row in f.readlines()]
with open(args.destmodule_file, 'r') as f:
	destnodes = [row.rstrip() for row in f.readlines()]

srcnodes = map(nodetype, srcnodes)
destnodes = map(nodetype, destnodes)

flows = flows.merge(flows_stats, left_on='NodeID', right_on='NodeID', how='outer')

flows['FCScore'] = (flows.Flow_value.values - flows.Flow_mean.values) / flows.Flow_std.values

flows[np.isnan(flows.FCScore) | np.isinf(flows.FCScore)].FCScore = None

print "Creating rich columns..."

# NOTE: this operation cannot be parallelized when gene symbols have to be retrieved on mygene server, probably the sock connection to the mygene server doesn't work in parallel

flows['Degree'] = flows.apply(lambda row: nx.degree(ppi, row.NodeID),axis=1)

flows['in_DestModule'] = flows.apply(lambda row: row.NodeID in destnodes,axis=1)
flows['in_SrcModule'] = flows.apply(lambda row: row.NodeID in srcnodes,axis=1)

print "Calculating distance statistics..."

spm = flower.load_spm(args.spm_file)

gm = utils.GIDMapper(spm['nodelist'])

destnodes_ids = gm.gid2id(destnodes)
srcnodes_ids = gm.gid2id(srcnodes)
ids = gm.gid2id(flows.NodeID.values)

flows['min_dist_dest'] = flower.eval_min_distance(spm['path_lengths'], destnodes_ids, ids)
flows['min_dist_src'] = flower.eval_min_distance(spm['path_lengths'], srcnodes_ids, ids)

flows['avg_dist_dest'] = flower.eval_avg_distance(spm['path_lengths'], destnodes_ids, ids)
flows['avg_dist_src'] = flower.eval_avg_distance(spm['path_lengths'], srcnodes_ids, ids)

print "Sorting..."

flows.sort_values('FCScore', ascending=False, inplace=True)

flows['Rank'] = range(len(flows))

flows = flows[['Rank','NodeID', 'FCScore', 'Flow_value', 'Degree', 'in_DestModule', 'in_SrcModule', 'min_dist_dest', 'min_dist_src', 'avg_dist_dest', 'avg_dist_src','Flow_mean', 'Flow_std', 'Flow_value_unnorm', 'Num_paths']]

print "Completed."

print flows.head()

print "Writing..."

flows.to_csv(args.output_file, sep='\t', index=False)