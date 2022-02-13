from abc import ABCMeta, abstractmethod
from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass

import pandas as pd
import itertools

from oagdedupe.mixin import BlockerMixin
from oagdedupe.base import BaseBlocker, BaseDistance, BaseTrain, BaseCluster
from oagdedupe.block.blockers import TestBlocker
from oagdedupe.train.threshold import Threshold
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
    trainer: Optional[BaseTrain] = Threshold()
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
        return

    def train(self):
        return

    def _get_candidates(self):
        
        block_map_list = self.blocker.get_block_maps(df=self.df)
        
        candidates = []
        for block_map in block_map_list:
            for x in block_map.values():
                candidates.extend(
                    [pair for pair in itertools.combinations(x, 2) if pair not in candidates]
                )

        return candidates

@dataclass
class RecordLinkage(BaseModel, BaseRecordLinkage):
    """General record linkage block, inherits from BaseModel.
    """

    def predict(self):
        return

    def train(self):
        return

    def _get_candidates(self):
        
        block_map_list1 = self.blocker.get_block_maps(df=self.df, attributes=self.attributes)
        block_map_list2 = self.blocker.get_block_maps(df=self.df2, attributes=self.attributes2)
        
        candidates_rl = []
        for block_map1, block_map2 in zip(block_map_list1, block_map_list2):
            joint_keys = [name for name in set(block_map1).intersection(set(block_map2))]
            for key in joint_keys:
                candidates_rl.extend(
                    [
                        pair 
                        for pair in self.blocker.product([block_map1[key], block_map2[key]], nodupes=False) 
                        if pair not in candidates_rl
                    ]
                )

        return candidates_rl
