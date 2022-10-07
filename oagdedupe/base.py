from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from oagdedupe._typing import StatsDict
from oagdedupe.block import base as block
from oagdedupe.settings import Settings


class BaseBlocking(ABC):
    def __init__(self):
        self.forward = block.BaseForward
        self.conj = block.BaseConjunctions
        self.pairs = block.BasePairs


class BaseDistance(ABC):
    """Abstract base class for all distance configurations to inherit"""

    @abstractmethod
    def compute_distances(self, pairs):
        return

    # @abstractmethod
    # def config(self):
    #     return


class BaseCluster(ABC):
    """Abstract base class for all clustering algos to inherit"""

    @abstractmethod
    def get_df_cluster(self):
        return
