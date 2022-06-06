from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property

import pandas as pd
import numpy as np
from sklearn.svm import SVC
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(rc={'figure.figsize':(11.7,8.27)})

from dedupe.base import BaseTrain

@dataclass
class Active(BaseTrain):
    """
    Model to implement active learning
    """

    def __post_init__(self):
        self.labels = {}
        self.samples = defaultdict(str)
        self.sorted_scores = defaultdict(list)

    def initialize(self, X):
        self.X = X
        self.train(self.X, init=True)
        self.scores, self.y = self.fit(self.X)

    def init_y(self, size):
        return np.random.choice([0,1],size=size, replace=True)

    def train(self, X, init=False, labels=None):
        X_scaled = StandardScaler().fit_transform(X)
        self.clf = SVC(kernel="linear", C=100, probability=True)
        if init==True:
            self.clf.fit(X_scaled, self.init_y(len(X)))
        else:
            self.clf.fit(X_scaled, labels)
        return self.clf

    def alternate_low_high(self, low):
        high = low[::-1]
        lowhigh = [None] * (len(low) + len(high))
        lowhigh[::2] = low
        lowhigh[1::2] = high
        return lowhigh

    def get_samples(self):
        self.dfX = (
            pd.DataFrame(self.X)
            .assign(scores=self.scores,y=self.y,uncertain=abs(self.scores-0.5))
            .reset_index()
            .sort_values("scores")
        )
        
        self.sorted_scores["init"] = self.alternate_low_high(self.dfX["scores"].values)
        self.sorted_scores["uncertain"] = self.dfX.sort_values("uncertain")["scores"].values

        unlabelled = self.dfX.loc[~self.dfX["index"].isin(self.active_dict.keys()),"index"]
        self.samples["init"] = self.alternate_low_high(unlabelled.values)
        self.samples["uncertain"] = self.dfX.sort_values("uncertain").loc[
            ~self.dfX["index"].isin(self.active_dict.keys()),"index"
        ].values

    @property
    def active_dict(self):
        return {
            int(x["c_index"]):x["label"] 
            for x in self.labels.values()
        }

    def retrain(self):
        self.train(
            self.X[list(self.active_dict.keys()),:], 
            init=False, 
            labels=list(self.active_dict.values())
        )
        self.scores, self.y = self.fit(self.X)

    def fit(self, X):
        X = StandardScaler().fit_transform(X)
        return self.clf.predict_proba(X)[:,1], self.clf.predict(X)