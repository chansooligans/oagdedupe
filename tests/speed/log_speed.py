# %%
from dedupe.api import Dedupe, RecordLinkage
from dedupe.block.blockers import NoBlocker
from dedupe.datasets.fake import df, df2

import pandas as pd
import numpy as np
import time

def timeis(func):
    '''Decorator that reports the execution time.'''
  
    def wrap(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
          
        print(func.__name__, round(end-start,2))
        print("\n")
        return result
    return wrap

dfcopied = pd.concat([df for _ in range(50)])
print(len(dfcopied))

d = Dedupe(df=dfcopied)


# %%
@timeis
def get_block_maps():
    return d.blocker.get_block_maps(df=d.df, attributes=d.attributes)

@timeis
def dedupe_get_candidates(block_maps):
    return d.blocker.dedupe_get_candidates(block_maps)

@timeis
def get_comparisons(idxmat):
    return d.distance.get_comparisons(d.df, d.df2, d.attributes, d.attributes2, idxmat)

@timeis
def get_distmat(comparisons):
    return np.column_stack([
            d.distance.p_distances(comparisons[attribute])
            for attribute in d.attributes
        ])

@timeis
def learn(X, idxmat):
    d.trainer.learn(d.df, X, idxmat)

@timeis 
def fit(X):
    scores, y = d.trainer.fit(X)

# %%
block_maps = get_block_maps()
idxmat = dedupe_get_candidates(block_maps)
comparisons = get_comparisons(idxmat)
X = get_distmat(comparisons)
learn(X, idxmat)
fit(X)

# %%
