# %%
from dedupe.api import Dedupe
# from dedupe.api import RecordLinkage
from dedupe.block.blockers import NoBlocker
from dedupe.datasets.fake import df, df2
from dedupe.api import Dedupe
from dedupe.distance.string import RayAllJaro
from dedupe.block import blockers 
from dedupe.block import algos
from dedupe.train.threshold import Threshold

import pandas as pd
import numpy as np
import time
import glob

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

files = glob.glob('/mnt/Research.CF/References & Training/Satchel/dedupe_rl/baseline_datasets/north_carolina_voters/*')

df = pd.concat([
    pd.read_csv(f)
    for f in files
])

attributes = ["givenname","surname","suburb","postcode"]
for attr in attributes:
    df[attr] = df[attr].astype(str)

df["suburb_postcode"] = df["suburb"] + df["postcode"]

manual_blocker = blockers.ManualBlocker([
    [
        (algos.FirstNLetters(N=3), "givenname"), 
        (algos.FirstNLetters(N=3), "surname"), 
        (algos.ExactMatch(), "suburb"), 
    ],
    [
        (algos.FirstNLetters(N=3), "givenname"), 
        (algos.FirstNLetters(N=3), "surname"), 
        (algos.ExactMatch(), "postcode"), 
    ],
])

# %%
d = Dedupe(
    df=df, 
    attributes=attributes, 
    blocker=manual_blocker,
    trainer=Threshold(threshold=0.95),
    distance=RayAllJaro(), 
    cpus=20
)

@timeis
def get_block_maps():
    return d.blocker.get_block_maps(df=d.df, attributes=d.attributes)

@timeis
def dedupe_get_candidates(block_maps):
    return d.blocker.dedupe_get_candidates(block_maps)

@timeis
def get_comparisons(idxmat):
    print(f"making {idxmat.shape[0]} comparions")
    return d.distance.get_comparisons(d.df, d.df2, d.attributes, d.attributes2, idxmat)

@timeis
def get_distmat(comparisons):
    return np.column_stack([
            d.distance.distance(comparisons[attribute])
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
