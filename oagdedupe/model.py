from abc import ABCMeta, abstractmethod
from typing import List, Union, Any, Optional, Dict
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
    attributes: List[str]
    blocker: Optional[BaseBlocker] = TestBlocker()
    distance: Optional[BaseDistance] = AllJaro()
    trainer: Optional[BaseTrain] = Threshold(threshold=0.85)
    cluster: Optional[BaseCluster] = ConnectedComponents()
    
    @abstractmethod
    def predict(self):
        """
        (1) Use trained model to identify matched candidates.
        (2) Use clustering algorithm to assign cluster IDs. Default is to define each connected component as a cluster.
        (3) Handle unclustered nodes.
        (4) Returns cluster IDs
        """
        candidates = self._get_candidates()
        return

    @abstractmethod    
    def train(self):
        """
        (1) Computes similarity scores for each column.
        (2) fit a model to learn p(match).
            - Default model is to average similarity scores and use a threshold of 0.9.
        (3) Return model that takes any dataframe or dataframes with same columns, and returns matched candidates.
        """
        return

    @abstractmethod
    def _get_candidates(self):
        """
        1) generate unique IDs;
        2) check if blocker selected
            - if blocker not selected, block map is all possible combinations of ID pairs;
            - else use blocker to get block map;
        3) use block map to generate pairs of candidate pairs of records
        """
        return

@dataclass
class BaseRecordLinkage:
    df2: pd.DataFrame
    attributes2: List[str]

@dataclass
class Dedupe(BaseModel):
    """General dedupe block, inherits from BaseModel.
    """

    def predict(self):
        idxmat, X = self.train()
        score, y = self.trainer.fit(X)
        clusters = self.cluster.cluster_ids(
            matches=idxmat[y==1].astype(int), 
            scores=score[y==1], 
            nobs=len(self.df), 
            rl=False
        )
        df_clusters = pd.DataFrame(clusters)
        df_clusters["x"] = df_clusters["id"].str.contains("x")
        df_clusters["id"] = df_clusters["id"].str.replace("x|y","").astype(float).astype(int)
        return df_clusters


    def train(self):
        candidates = []
        for idx,idy in self._get_candidates():
            distances = []
            for attribute in self.attributes:
                distances.append(
                    self.distance.distance(self.df[attribute].iloc[idx], self.df[attribute].iloc[idy])
                )
            candidates.append([idx, idy] + distances)

        candidates = np.array(candidates)
        self.trainer.learn(candidates[:,2:])
        
        return candidates[:,:2],candidates[:,2:]

    def _get_candidates(self):
        return self.blocker.dedupe_get_candidates(
            self.blocker.get_block_maps(df=self.df)
        )

@dataclass
class RecordLinkage(BaseModel, BaseRecordLinkage):
    """General record linkage block, inherits from BaseModel.

    Keep this in mind when coding Dedupe, but update once Dedupe is done.
    """

    def predict(self):
        return

    def train(self):
        return

    def _get_candidates(self):
        
        block_maps1, block_maps2 = [
            self.blocker.get_block_maps(df=_)
            for _ in [self.df, self.df2]
        ]
        
        return self.blocker.rl_get_candidates(
            block_maps1, block_maps2
        )
