from pathlib import Path
import os
import json
from dedupe.api import Dedupe, RecordLinkage
from dedupe.train.active import Active
from collections import defaultdict
import pandas as pd

class Init:

    def __init__(self, cache_path):
        self.cache_path = cache_path

    def _load_dataset(self, datset_filename, lab):
        print("initializing")
        self.lab = lab
        self.datset_filename = datset_filename
        self.df = pd.read_csv(self.datset_filename)
        self.setup_dedupe(self.df)
        self.d.trainer.labels = self.lab.labels

    def setup_cache(self):
        os.makedirs(self.cache_path, exist_ok=True) 
        label_path =  Path(f"{self.cache_path}/samples.json")
        meta_path =  Path(f"{self.cache_path}/meta.json") 
        if not label_path.is_file():
            with open(f"{self.cache_path}/samples.json", "w") as f:
                json.dump({}, f)

        if not meta_path.is_file():
            with open(f"{self.cache_path}/meta.json", "w") as f:
                json.dump({}, f)

    def setup_dedupe(self, df):
        self.d = Dedupe(df=df, trainer=Active())
        self.idxmat = self.d._get_candidates()
        X = self.d.distance.get_distmat(
            self.d.df, 
            self.d.df2, 
            self.d.attributes, 
            self.d.attributes2, 
            self.idxmat
        )
        self.d.trainer.initialize(X)
        return self.d, self.idxmat