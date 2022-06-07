from dataclasses import dataclass
import numpy as np
from sklearn.svm import SVC
from sklearn.preprocessing import StandardScaler

from dedupe.base import BaseTrain

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

