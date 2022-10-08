# Generate Training Samples

First, we load the dataframe to shema.df. 

Second, we generate training samples which consist of three parts, which we name "positive", "negative" and "unlabelled" samples:
1. positive samples: a single sample repeated 4 times
2. negative samples: 10 random samples
3. unlabeled samples: a sample size of `settings.model.n`

These n + 10 + 4 records are loaded into schema.train.

Third, we create schema.labels, which contains nC2 comparison pairs generated using the positive and negative samples. 
The pairs from positive samples are labeled as a match, while the pairs from negative samples are labeled as a non-match.

Finally, compute distances between comparison pairs. If there are 3 attributes (e.g. name, address, age), there would be 3 separate 
distance computations.
