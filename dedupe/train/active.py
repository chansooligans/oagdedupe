from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property

import pandas as pd
import numpy as np
from modAL.models import ActiveLearner
from functools import partial
from modAL.batch import uncertainty_batch_sampling
from sklearn.ensemble import RandomForestClassifier

import pandas as pd
from IPython import display
import json
import time
import os

import matplotlib.pyplot as plt
import seaborn as sns
sns.set(rc={'figure.figsize':(11.7,8.27)})

from dedupe.base import BaseTrain

@dataclass
class Active(BaseTrain):
    """
    Model to implement active learning
    """
    cache_fp:str = "../cache/test.csv"

    def __post_init__(self):
        
        # Pre-set our batch sampling to retrieve 3 samples at a time.
        self.BATCH_SIZE = 5
        preset_batch = partial(uncertainty_batch_sampling, n_instances=self.BATCH_SIZE)

        # initializing the learner
        self.clf = ActiveLearner(
            estimator=RandomForestClassifier(),
            query_strategy=preset_batch,

        )


    def query(self, df, X, idxmat, queried):
        
        query_index, query_instance = self.clf.query(np.delete(X, queried, axis=0))
        query_index = np.delete(self.indices, queried, axis=0)[query_index]

        samples = pd.concat([
                (
                    df
                    .loc[idxmat[query_index,0]]
                    .reset_index(drop=True)
                    .set_axis(["name_l","addr_l"], axis=1)
                ),
                (
                    df
                    .loc[idxmat[query_index,1]]
                    .reset_index(drop=True)
                    .set_axis(["name_r","addr_r"], axis=1)
                )
            ], axis=1
        )
        samples['idx'] = query_index
        samples["label"] = 0
        samples = samples[
            ["label"]+[x for x in samples.columns if x != "label"]
        ]
        
        if os.path.exists(self.cache_fp):
            samples.to_csv(
                self.cache_fp, 
                mode='a',
                header=False,
                index=False
            )
        else:
            samples.to_csv(
                self.cache_fp, 
                index=False
            )

        return query_index

    @cached_property
    def output_map(self):
        return {
            1:1,
            2:0
        }


    def update_model(self, X):
        if os.path.exists(self.cache_fp):
            samples = pd.read_csv(self.cache_fp).drop_duplicates()
            samples = samples.loc[samples["label"].isin([1,2])]
            samples["label"] = samples["label"].map(self.output_map)
            queried = list(samples["idx"].values)
            self.clf.teach(X=X[queried], y=samples["label"].values)
        else:
            queried = list()
        return queried

    def learn(self, df, X, idxmat):

        self.indices = np.array(range(len(X)))

        if os.path.exists(self.cache_fp):
            queried = self.update_model(X)
        else:
            queried = list()
 
        while True:
            
            query_index = self.query(df, X, idxmat, queried)
            
            resp = input("Click enter once batch complete. Enter 'exit' to finish learning.")

            if resp == "exit":
                break
            
            queried = self.update_model(X)
            
    def fit(self, X):
        return self.clf.predict_proba(X),self.clf.predict(X)