# Overview

## Intro

This package is for entity resolution at scale.

There are two types of entity resolution: deduplication and record linkage. Deduplication finds common entities in a single dataset. Record linkage finds common entities between a pair of datasets.


## Terms

- comparison pairs:
- block scheme:
- signature:
- forward index:
- inverted index:
- block conjunction:
- reduction ratio:
- coverage:


## Overview

1. Initialize training sample
2. Active Learning Loop:
	a. Train block learner
	b. Train classifier
4. Cluster linkages

## Set Up

To run this package, you need four things:
1. the dataframe to dedupe
2. a postgres database 
3. label-studio (used for labeling active learning samples)
4. settings

The easiest way to set up postgres and label-studio is with docker:

[docker commands]

[settings]


## Generate Training Samples

First, we load the dataframe to shema.df. 

Second, we generate training samples which consist of three parts, which we name "positive", "negative" and "unlabelled" samples:
1. positive samples: a single sample repeated 4 times
2. negative samples: 10 random samples
3. unlabeled samples: a sample size of `settings.other.n`

These n + 10 + 4 records are loaded into schema.train.

Third, we create schema.labels, which contains nC2 comparison pairs generated using the positive and negative samples. 
The pairs from positive samples are labeled as a match, while the pairs from negative samples are labeled as a non-match.

Finally, compute distances between comparison pairs. If there are 3 attributes (e.g. name, address, age), there would be 3 separate 
distance computations.


## Blocking

Naive entity resolution is a O(n^2) problem. For each record, there are n-possible records that it may be linked with. Common ways to reduce complexity is to use blocking. As an example, you might only consider pairs that share the same first 3 characters. We call the amount that blocking reduces complexity the "reduction ratio". If a blocking scheme generates 1,000,000 comparisons on a deduplication of 100,000 records, the reduction ratio is 99.98% (There are ~10,000**2 / 2 possible comparisons). At scale, a single block scheme will likely not yield a sufficiently large reduction ratio and you may want a conjunction of blocking schemes.

The goal is of each active learning loop is to identify the "best" union of blocking conjunctions. 

First, to manage scalability, we use a sample of data to learn the optimal blocking conjunctions, using a new sample with each active learning loop. 

Second, we create a forward index on the sample. That is, we build schema.blocks_train where each row is an entity and each column is a block scheme. The values are the signatures for the corresponding entity and block scheme.

Third, for each block scheme, we use dynamic programming to greedily search for the best conjunction. 

To identify the "best" conjunction, we consider the reduction ratio, positive coverage, and negative coverage. While excluding any conjunction that does not capture any positive pairs, we want to (1) maximize the reduction ratio, (2) maximize the positive coverage, and (3) minimize the negative coverage -- in that order. To reduce complexity further, we limit the length of the conjunctions to `settings.other.k` conjunctions. We find k=3 is usually sufficient.

Fourth, we apply the blocking conjunctions to obtain comparison pairs. Iterating from best-to-worst conjunctions, we obtain comparison pairs until either stopping condition: (1) we have obtained `n_covered` pairs or (2) the conjunction would yield too many comparisons. This latter condition typically only applies when we fetch comparison pairs from the full data, and is not a concern when learning blocking from the sample.

## Distance

Once we have comparison pairs, we compute distances between each pairs' attributes.

## Classifier

Entity resolution is a binary classification problem, where each unit is a comparison pair, X is a matrix of distances between attributes, and the output is either a match or a non-match.

We use modAl, sklearn random forest classifier, and fastAPI. 

The first iteration trains the model using the fake data constructed from the sample at initialization. Uncertainty sampling is applied and samples are sent to label-studio for manual label.

## Active Learning Loop

As uncertainty samples are labeled, label-studio sends labels to fastAPI. When there are fewer than 5 tasks left on label-studio, fastAPI generates new samples.

To generate new samples, we first append the newly labeled pairs to schema.labels. Next we delete all records in schema.train that have not been labeled. A new sample of size settings.other.n is pulled and appended to schema.train.

Next, we re-evaluate blocks to obtain best block conjunctions. The metrics are now computed on this new sample and the updated labels are used to evaluate positive and negative coverage.

Again, we apply the block conjunctions to get comparison pairs then re-train the active learner with newly labeled data. Finally, the active learner submits new uncertain samples to label-studio.

## Clustering

Once active learning is completed, we have a trained block learner and classifier. Next, we apply these to the full data set. We get comparison pairs using the blocking conjunctions we have learned, then predict match probabilities using the trained classification model.

The output of the classifier is a dataframe where each row is a comparison pair with a match probability. We load this data to a graph database, where each entity is a node and each pair is a linkage. The score is a linkage weight.

A connected components algorithm is used to create cluster IDs. The final output is a dataframe with records for which a linkage was found. A new column "cluster" contains the cluster ID and linked records share a common cluster ID.