from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass

import numpy as np

from oagdedupe.base import BaseTrain

@dataclass
class Threshold(BaseTrain):
    threshold: float = 0.8

    def learn(self, X):
        return

    def fit(self, X):
        means = X.mean(axis=1)
        return np.where(means>self.threshold, 1, 0)
