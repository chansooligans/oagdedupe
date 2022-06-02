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

    def get_samples(self):
        self.dfX = (
            pd.DataFrame(self.X)
            .assign(scores=self.scores,y=self.y,uncertain=abs(self.scores-0.5))
            .reset_index()
            .sort_values("scores")
        )

        unlabelled = self.dfX.loc[~self.dfX["index"].isin(self.active_dict.keys()),"index"]

        low = unlabelled[:10].values
        high = unlabelled[-10:].values
        lowhigh = [None] * (len(low) + len(high))
        lowhigh[::2] = low
        lowhigh[1::2] = high
        
        self.samples["init"] = lowhigh
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