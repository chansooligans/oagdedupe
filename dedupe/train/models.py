from typing import List, Union, Any, Optional, Dict
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
from dedupe.utils import timing

@dataclass
class TestTrain(BaseTrain):
    """
    Model to be used only for testing Dedupe with test dataframes.
    Uses pre-labelled classes to train overfit svm. 
    """
    threshold: float = 0.8

    @property
    def y(self):
        return np.array([1, 1, 0, 1, 0, 1, 1, 1, 0, 1, 0, 1, 1, 1, 1])

    def learn(self, df, X, candidates):
        self.X_scaled = StandardScaler().fit_transform(X)
        self.clf = SVC(kernel="linear", C=100, probability=True)
        self.clf.fit(self.X_scaled, self.y)
        return self.clf

    def fit(self, X):
        return self.clf.predict_proba(self.X_scaled)[:,1], self.clf.predict(self.X_scaled)

@dataclass
class Threshold(BaseTrain):
    """
    Naive model that computes means of distances for each candidate pair.
    Then labels all pairs whose mean distance > threshold as a match.
    """
    threshold: float = 0.8

    def learn(self, df, X, candidates):
        return

    def fit(self, X):
        means = X.mean(axis=1)
        labels = np.where(means>self.threshold, 1, 0)
        return means, labels

@dataclass
class Active(BaseTrain):
    """
    Model to implement active learning
    """

    @cached_property
    def active_dict(self):
        return defaultdict(int)

    def initialize(self, X):
        self.X = X
        self.train(self.X, init=True)
        self.scores, self.y = self.fit(self.X)

    @cached_property
    def samples(self):
        return {
            "lowest":None,
            "highest":None,
            "uncertain":None
        }

    @cached_property
    def labels(self):
        return {
            "lowest":None,
            "highest":None,
            "uncertain":None
        }

    @property
    def init_y(self):
        return np.random.choice([0,1],size=self.X_scaled.shape[0], replace=True)

    def train(self, X, init=False, labels=None):
        self.X_scaled = StandardScaler().fit_transform(X)
        self.clf = SVC(kernel="linear", C=100, probability=True)
        if init==True:
            self.clf.fit(self.X_scaled, self.init_y)
        else:
            self.clf.fit(self.X_scaled, labels)
        return self.clf

    def get_samples(self):
        self.dfX = (
            pd.DataFrame(self.X)
            .assign(scores=self.scores,y=self.y,uncertain=abs(self.scores-0.5))
            .reset_index()
            .sort_values("scores")
        )

        unlabelled = self.dfX.loc[~self.dfX["index"].isin(self.active_dict.keys()),"index"]

        self.samples["lowest"] = unlabelled[:5].values
        self.samples["highest"] = unlabelled[-5:].values
        self.samples["uncertain"] = self.dfX.sort_values("uncertain").loc[
            ~self.dfX["index"].isin(self.active_dict.keys()),"index"
        ][:5].values

    def active_learn(self, _type, candidates, df):
        self.labels[_type] = []
        for a,b in candidates[self.samples[_type],:]:
            while True:
                try:
                    userinput = int(input(f"{df.loc[a]} \n\n {df.loc[b]}"))
                except ValueError:
                    print("Needs to be 1 or 0")
                    continue
                else:
                    break
            self.labels[_type].append(userinput)

    def update_active_dict(self,_type):
        if _type == "uncertain":
            indices = list(self.samples["uncertain"])
            labels = self.labels["uncertain"] 
        else:
            indices = list(self.samples["lowest"]) + list(self.samples["highest"])
            labels = self.labels["lowest"] + self.labels["highest"]
        for idx,lab in zip(indices, labels):
            self.active_dict[idx] = lab

    def retrain(self):
        self.train(
            self.X[list(self.active_dict.keys()),:], 
            init=False, 
            labels=list(self.active_dict.values())
        )
        self.scores, self.y = self.fit(self.X)

    def run(self,candidates, df ,_type="lowhigh"):
        self.get_samples()
        if _type == "lowhigh":
            for x in ["lowest","highest"]:
                self.active_learn(x, candidates, df)
        else:
            self.active_learn("uncertain", candidates, df)
        self.update_active_dict(_type)
        self.retrain()
        plt.figure()
        sns.scatterplot(self.X[:,0],self.X[:,1], hue=self.scores)
        plt.figure()
        sns.scatterplot(self.X[:,0],self.X[:,1], hue=self.y)
        plt.show()

    def learn(self, df, X, candidates):
        self.initialize(X)
        self.run(candidates=candidates, df = df, _type="lowhigh")
        user_continue = True
        while user_continue:
            self.run(candidates=candidates, df = df, _type="uncertain")
            user_continue = input()

    def fit(self, X):
        X = StandardScaler().fit_transform(X)
        return self.clf.predict_proba(X)[:,1], self.clf.predict(X)
