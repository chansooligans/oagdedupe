# Intro

This package handles two types of entity resolution: deduplication and record linkage. 
Deduplication finds common entities in a single dataset. 
Record linkage finds common entities between a pair of datasets. 

"Common entity" means that the data represent the same underlying person, place, or thing; 
but there are differences in their representations. For example, ["George McDonald", "28 Liberty St"] 
and ["George S. McDonalds", "28 Liberty Street"] may be the same person, 
with a slightly spelled surname and abbreviated street.

This package identifies commonness based on string and/or numeric similarity, 
which is useful where data contain errors due to transcription, pdf extraction, 
data entry, etc. This package does not identify commonness between records where 
fields are not similar at all, e.g. someone who changed their name and 
address and appear in the dataset twice would not be deduped.

#### Scalability

A challenge with entity resolution is that it is a O(n^2) problem. 
For each record, there are n-possible records that it may be linked with. 
This introduces both space and time constraints particularly when working 
with large datasets. 

To mitigate his problem, (1) we use a common solution called blocking to learn 
the most efficient block conjunctions to generate comparison pairs and 
(2) run most of the pipeline out of memory, in a postgres database.

#### Active Learning

Once comparison pairs are generated, we have a binary classification problem 
where the output is either a match or not a match. There are several 
unsupervised and Bayesian approaches that can solve this problem, but we believe 
active learning performs best, even if it does require human review. 
To facilitate human review, we have incorporated label-studio into our pipeline.

#### Pipeline Overview

A quick overview of the pipeline:

1. Initialize training sample
2. Active Learning Loop:
	a. Train block learner to learn best block scheme conjunctions
	b. Train binary classifier to learn 
3. Run trained block learner and classifier on full data
4. Cluster linkages 

Also see diagram on [active_learn_loop](active_learn_loop)