from abc import ABC, abstractmethod
from functools import cached_property
from typing import List, Optional, Tuple

from oagdedupe._typing import StatsDict
from oagdedupe.block import base as block
from oagdedupe.settings import Settings


# db
class BaseComputeBlocking(ABC):
    pass


class BaseCompute(ABC):
    @cached_property
    @abstractmethod
    def blocking(self) -> BaseComputeBlocking:
        return

    @abstractmethod
    def setup(self):
        """sets up environment

        creates:
        - df
        - pos
        - neg
        - unlabelled
        - train
        - labels
        """
        return

    @abstractmethod
    def label_distances(self):
        """computes distances for labels"""
        return


# blocking
class BaseBlocking(ABC):
    def __init__(self):
        self.forward = block.BaseForward
        self.conj = block.BaseConjunctions
        self.pairs = block.BasePairs


# distance
class BaseDistance(ABC):
    """Abstract base class for all distance configurations to inherit"""

    @abstractmethod
    def save_distances(self, table, newtable):
        return


# cluster
class BaseCluster(ABC):
    """Abstract base class for all clustering algos to inherit"""

    @abstractmethod
    def get_df_cluster(self):
        return
