from abc import ABCMeta, abstractmethod
from typing import List, Union, Any, Optional, Dict, Tuple
from dataclasses import dataclass

import pandas as pd
import numpy as np
import itertools

from oagdedupe.mixin import BlockerMixin
from oagdedupe.base import BaseBlocker, BaseDistance, BaseTrain, BaseCluster
from oagdedupe.block.blockers import TestBlocker
from oagdedupe.train.models import Threshold
from oagdedupe.distance.string import AllJaro
from oagdedupe.cluster.cluster import ConnectedComponents

@dataclass
class BaseModel(metaclass=ABCMeta):
    """ Abstract base class from which all model classes inherit.
    All descendent classes must implement predict, train, and candidates methods.
    """
    df: pd.DataFrame
    attributes: Optional[List[str]] = None
    attributes2: Optional[List[str]] = None
    blocker: Optional[BaseBlocker] = TestBlocker()
    distance: Optional[BaseDistance] = AllJaro()
    trainer: Optional[BaseTrain] = Threshold(threshold=0.85)
    cluster: Optional[BaseCluster] = ConnectedComponents()
    
    @abstractmethod
    def predict(self):
        candidates = self._get_candidates()
        return

    @abstractmethod    
    def fit(self):
        return

    @abstractmethod
    def _get_candidates(self):
        return

@dataclass
class BaseRecordLinkage:
    df2: pd.DataFrame

@dataclass
class Dedupe(BaseModel):
    """General dedupe block, inherits from BaseModel.
    """
    
    def __post_init__(self):
        if self.attributes is None:
            self.attributes = self.df.columns

    def predict(self) -> pd.DataFrame:
        """get clusters of matches and return cluster IDs"""
        
        idxmat, scores, y = self.fit()
        return self.cluster.get_df_cluster(
            matches=idxmat[y==1].astype(int), 
            scores=scores[y==1],
            rl=False
        )

    def fit(self) -> Tuple[np.array, np.array, np.array]:
        """learn p(match)"""
        idxmat = self._get_candidates()
        X = self.distance.get_distmat(self.df, self.attributes, idxmat)
        self.trainer.learn(X)
        scores, y = self.trainer.fit(X)
        return idxmat, scores, y

    def _get_candidates(self) -> np.array:
        """get candidate pairs"""
        block_maps = self.blocker.get_block_maps(df=self.df)
        
        return self.blocker.dedupe_get_candidates(
            block_maps
        )

@dataclass
class RecordLinkage(Dedupe, BaseModel, BaseRecordLinkage):
    """General record linkage block, inherits from BaseModel.
    """

    def __post_init__(self):
        if (self.attributes is None) & (self.attributes2 is None):
            unq_cols = list(set(self.df.columns).intersection(self.df2.columns))
            self.attributes = self.attributes2 = unq_cols
        elif self.attributes2 is None:
            self.attributes2 = self.attributes

    def predict(self) -> pd.DataFrame:
        """get clusters of matches and return cluster IDs"""
        
        idxmat, scores, y = self.fit()
        return self.cluster.get_df_cluster(
            matches=idxmat[y==1].astype(int), 
            scores=scores[y==1],
            rl=True
        )

    def fit(self) -> Tuple[np.array, np.array, np.array]:
        """learn p(match)"""
        idxmat = self._get_candidates()
        X = self.distance.get_distmat_rl(self.df, self.df2, self.attributes, self.attributes2, idxmat)
        self.trainer.learn(X)
        scores, y = self.trainer.fit(X)
        return idxmat, scores, y

    def _get_candidates(self) -> np.array:
        "get candidate pairs"
        
        block_maps1, block_maps2 = [
            self.blocker.get_block_maps(df=_)
            for _ in [self.df, self.df2]
        ]
        
        return self.blocker.rl_get_candidates(
            block_maps1, block_maps2
        )
