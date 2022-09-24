# Intro

This package is for entity resolution at scale.

There are two types of entity resolution: deduplication and record linkage. Deduplication finds common entities in a single dataset. Record linkage finds common entities between a pair of datasets.

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