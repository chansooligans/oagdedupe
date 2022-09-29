# Blocking

Common ways to reduce complexity is to use blocking. As an example, you might only consider pairs that share the same first 3 characters. We call the amount that blocking reduces complexity the "reduction ratio". If a blocking scheme generates 1,000,000 comparisons on a deduplication of 100,000 records, the reduction ratio is 99.98% (There are ~10,000**2 / 2 possible comparisons). At scale, a single block scheme will likely not yield a sufficiently large reduction ratio and you may want a conjunction of blocking schemes.

The goal is of each active learning loop is to identify the "best" union of blocking conjunctions. 

First, to manage scalability, we use a sample of data to learn the optimal blocking conjunctions, using a new sample with each active learning loop. 

Second, we create a forward index on the sample. That is, we build schema.blocks_train where each row is an entity and each column is a block scheme. The values are the signatures for the corresponding entity and block scheme.

Third, for each block scheme, we use dynamic programming to greedily search for the best conjunction. 

To identify the "best" conjunction, we consider the reduction ratio, positive coverage, and negative coverage. While excluding any conjunction that does not capture any positive pairs, we want to (1) maximize the reduction ratio, (2) maximize the positive coverage, and (3) minimize the negative coverage -- in that order. To reduce complexity further, we limit the length of the conjunctions to `settings.other.k` conjunctions. We find k=3 is usually sufficient.

Fourth, we apply the blocking conjunctions to obtain comparison pairs. Iterating from best-to-worst conjunctions, we obtain comparison pairs until either stopping condition: (1) we have obtained `n_covered` pairs or (2) the conjunction would yield too many comparisons. This latter condition typically only applies when we fetch comparison pairs from the full data, and is not a concern when learning blocking from the sample.