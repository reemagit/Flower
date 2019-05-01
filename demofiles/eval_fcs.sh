python ../evaluate_spm.py demograph.edgelist spm.h5
python ../evaluate_flows.py demograph.edgelist source.txt target.txt spm.h5 flows.tsv
python ../evaluate_flows_statistics.py demograph.edgelist rdm_samples.txt spm.h5 flows_stats.tsv
python ../evaluate_rankings.py flows.tsv flows_stats.tsv demograph.edgelist source.txt target.txt spm.h5 flows_rank.tsv
