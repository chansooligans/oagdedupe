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
    def save_distances(self, table, newtable):
        return


class BaseCluster(ABC):
    """Abstract base class for all clustering algos to inherit"""

    @abstractmethod
    def get_df_cluster(self):
        return


class BaseORM(ABC):
    """Abstract base class for all ORMs to inherit"""

    pass
