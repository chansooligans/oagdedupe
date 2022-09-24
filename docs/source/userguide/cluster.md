# Clustering

Once active learning is completed, we have a trained block learner and classifier. Next, we apply these to the full data set. We get comparison pairs using the blocking conjunctions we have learned, then predict match probabilities using the trained classification model.

The output of the classifier is a dataframe where each row is a comparison pair with a match probability. We load this data to a graph database, where each entity is a node and each pair is a linkage. The score is a linkage weight.

A connected components algorithm is used to create cluster IDs. The final output is a dataframe with records for which a linkage was found. A new column "cluster" contains the cluster ID and linked records share a common cluster ID.