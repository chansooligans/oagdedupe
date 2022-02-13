# Steps:

1. Create deduping object, inputs:
    - df  
    - columns  
2. Block then get block maps.
3. Use block map to get candidates, then compute similarity scores for each column
4. Use scores to learn p(match)
5. Predict p(match) on data then keep matches
6. Get clusters of connected components using a graph
7. Handle unclustered nodes (own cluster ID or None)
8. Return cluster IDs

# Organization

- oagdedupe
    - model
        - BaseModel(ABC)
            - block_map, candidates, train, predict
        - Dedupe(BaseModel):
            - def __init__:
                - dataframe
                - columns
                - default blocking algo or user input
                - default distance algo or user input 
                - default training algo or user input 
                - default clustering algo or user input 
            - def block_map:
                "use raw data to get block maps"
                return block_map
            - def candidates:
                "use block_map to get candidates"
                return candidates
            - def learn:
                "use candidates to train model to learn P(match)"
                return trained_model
            - def predict:
                "apply trained model to data"
                "use labelled pairs to get connected components"
                return cluster_ids
        - RecordLinkage(BaseModel):
            - def __init__:
                - dataframe1, dataframe2
                - columns1, columns2
                - default blocking algo or user input
                - default distance algo or user input 
                - default training algo or user input 
                - default clustering algo or user input 
            - def block_map:
                "use raw data to get block maps"
                return block_maps
            - def candidates:
                "use block_map to get candidates"
                return candidates
            - def learn:
                "use candidates to train model to learn P(match)"
                return trained_model
            - def predict:
                "apply trained model to data"
                "use labelled pairs to get connected components"
                return cluster_ids
    - base.py
        - BaseBlock: abstraction for block creator
        - BaseBlockAlgos: abstraction for blocking algos
        - BaseDistance: abstraction for string distance calculations
        - BaseTrain: abstraction for learning algorithms
        - BaseCluster: abstraction for clustering algos
    - block: classes that generate block maps
        - ManualBlock.py (user defined blocks)
        - AutoBlock.py (smart block creation)
        - Algos: subfolder containing blocking algorithms
            - firstletter, ooneGramFingerPrint, commonFourGram, etc...
    - distance: classes that get string distances
        - string
            - simple ratio, partial ratio, token sort ratio, affine gap distance, 
            jaro-winkler, hamming, tfidf, softtfidf, monge-elkan, extended jaccard
        - numeric
        - date, age, time
        - geographic distance
    - learn: classes that label pairs as matches 
        - threshold
        - supervised
        - activelearn
        - unsupervised
    - cluster: 
        - connected components

Usage:

```
from oagdedupe import Dedupe

model = Dedupe(df)
df["cluster_id"] = model.predict()
```

# Terms:

- Blocking
    - Block Union: a union of Block Intersections
    - Block Intersection: an intersection of Block Methods
    - Block Method: a Pair consisting of a method and column name
    - Block Map: a dictionary where keys are cartesian product of the block IDs
    for each blocking method and values are record IDs; as an example, if the 
    block is first letter of "address" column and first letter of "name" column, then 
    one key may be "A-B" for records where "name" begins with "A" AND "address" begins with "B" 
    and the value would be a list of record IDs
    - Block ID: the keys of a block map (e.g. if a block key is "A-B" for records where "name" begins with "A" 
    and "address" begins with "B", "A-B" is a block ID)
- Candidate: a candidate pair of records that may be the same true entity
- Cluster ID: a unique identifier for same true entity
