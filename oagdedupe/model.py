from abc import ABCMeta, abstractmethod
from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass
from oagdedupe.mixin import BlockerMixin
from oagdedupe.block.blockers import ManualBlocker
from oagdedupe.train.threshold import Threshold
from oagdedupe.distance.string import AllJaro
from oagdedupe.cluster.cluster import ConnectedComponents

class BaseModel(metaclass=ABCMeta):
    """ Abstract base class from which all model classes inherit.
    All descendent classes must implement predict, train, and candidates methods.
    """
    blocker = ManualBlocker()
    distance = AllJaro()
    trainer = Threshold()
    cluster = ConnectedComponents()
    
    @abstractmethod
    def predict(self,df, cols):
        """
        Use trained model to identify matched candidates.
        Use clustering algorithm to assign cluster IDs. Default is to define each connected component as a cluster.
        Handle unclustered nodes.
        Returns cluster IDs
        """
        return

    @abstractmethod    
    def train():
        """
        Computes similarity scores for each column.
        Then fits a model to learn p(match).
        Default model is to average similarity scores and use a threshold of 0.9.
        Return model that takes any dataframe or dataframes with same columns, and returns matched candidates.
        """
        return

    @abstractmethod
    def get_candidates(self):
        """
        Returns candidate pairs of records. Blocking is optional.
        """
        return

@dataclass
class Dedupe(BaseModel, BlockerMixin):
    """General dedupe block, inherits from BaseModel.
    """

    def predict(self,df, cols):
        return

    def train(self):
        return

    def get_candidates(self):
        return

@dataclass
class RecordLinkage(BaseModel, BlockerMixin):
    """General record linkage block, inherits from BaseModel.
    """

    def predict(self,df, cols):
        return

    def train():
        return

    def get_candidates(self):
        return
