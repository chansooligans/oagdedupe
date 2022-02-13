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
    def predict(self, df, cols):
        """
        (1) Use trained model to identify matched candidates.
        (2) Use clustering algorithm to assign cluster IDs. Default is to define each connected component as a cluster.
        (3) Handle unclustered nodes.
        (4) Returns cluster IDs
        """
        return

    @abstractmethod    
    def train():
        """
        (1) Computes similarity scores for each column.
        (2) fit a model to learn p(match).
            - Default model is to average similarity scores and use a threshold of 0.9.
        (3) Return model that takes any dataframe or dataframes with same columns, and returns matched candidates.
        """
        return

    @abstractmethod
    def get_candidates(self):
        """
        1) generate unique IDs;
        2) check if blocker selected
            - if blocker not selected, block map is all possible combinations of ID pairs;
            - else use blocker to get block map;
        3) use block map to generate pairs of candidate pairs of records
        """
        return

@dataclass
class Dedupe(BaseModel, BlockerMixin):
    """General dedupe block, inherits from BaseModel.
    """

    def predict(self, df, cols):
        return

    def train(self):

        return

    def get_candidates(self):
        return

@dataclass
class RecordLinkage(BaseModel, BlockerMixin):
    """General record linkage block, inherits from BaseModel.
    """

    def predict(self, df, cols):
        return

    def train():
        return

    def get_candidates(self):
        return
