from pathlib import Path
import os
import json
from dedupe.api import Dedupe, RecordLinkage
from dedupe.train.active import Active
from collections import defaultdict

def setup_cache(cache_path):
    os.makedirs(cache_path, exist_ok=True) 
    label_path =  Path(f"{cache_path}/samples.json")
    meta_path =  Path(f"{cache_path}/meta.json") 
    if not label_path.is_file():
        with open(f"{cache_path}/samples.json", "w") as f:
            json.dump({}, f)

    if not meta_path.is_file():
        with open(f"{cache_path}/meta.json", "w") as f:
            json.dump({}, f)

def setup_dedupe(df):
    d = Dedupe(df=df, trainer=Active())
    idxmat = d._get_candidates()
    X = d.distance.get_distmat(d.df, d.df2, d.attributes, d.attributes2, idxmat)
    d.trainer.initialize(X)
    d.trainer.get_samples()
    return d, idxmat