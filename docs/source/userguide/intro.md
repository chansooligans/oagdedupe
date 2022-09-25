# Intro

There are two types of entity resolution: deduplication and record linkage. Deduplication finds common entities in a single dataset. Record linkage finds common entities between a pair of datasets.

A quick overview of the pipeline include the steps:

1. Initialize training sample
2. Active Learning Loop:
	a. Train block learner to learn best block scheme conjunctions
	b. Train binary classifier to learn 
3. Run trained block learner and classifier on full data
4. Cluster linkages 
