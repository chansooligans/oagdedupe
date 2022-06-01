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
        self.samples = defaultdict(str)
        self.labels = defaultdict(str)
        self.active_dict = defaultdict(int)
        self.init_y = np.random.choice([0,1],size=self.X_scaled.shape[0], replace=True)

    def get_samples(self):
        df = pd.read_csv(f"{self.fp}/train.csv")
        unlabelled = df.loc[~df["index"].isin(self.active_dict.keys()),"index"]
        self.samples["lowest"] = unlabelled[:5].values
        self.samples["highest"] = unlabelled[-5:].values
        self.samples["uncertain"] = df.sort_values("uncertain").loc[
            ~df["index"].isin(self.active_dict.keys()),"index"
        ][:5].values

    def initialize(self, X):
        self.X = X
        self.train(self.X, init=True)
        self.scores, self.y = self.fit(self.X)

        (
            pd.DataFrame(self.X)
            .assign(scores=self.scores,y=self.y,uncertain=abs(self.scores-0.5))
            .reset_index()
            .sort_values("scores")
        ).to_csv(f"{self.fp}/train.csv")

    def train(self, X, init=False, labels=None):
        self.X_scaled = StandardScaler().fit_transform(X)
        self.clf = SVC(kernel="linear", C=100, probability=True)
        if init==True:
            self.clf.fit(self.X_scaled, self.init_y)
        else:
            self.clf.fit(self.X_scaled, labels)
        return self.clf

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

    def learn(self, df, X, candidates):
        self.initialize(X)
        self.run(candidates=candidates, df = df, _type="lowhigh")
        user_continue = True
        while user_continue:
            self.run(candidates=candidates, df = df, _type="uncertain")
            user_continue = input()
