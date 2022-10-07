from abc import ABCMeta, abstractmethod
from typing import List, Optional, Tuple

from oagdedupe._typing import StatsDict


class BaseOptimizer(metaclass=ABCMeta):
    """Abstract base class for all conjunction optimizing algorithms to inherit"""

    @abstractmethod
    def get_best(self, scheme: Tuple[str]) -> Optional[List[StatsDict]]:
        return


class BaseBlockAlgo(metaclass=ABCMeta):
    """Abstract base class for all blocking algos to inherit"""

    @abstractmethod
    def get_block(self):
        return


class BaseDistance(metaclass=ABCMeta):
    """Abstract base class for all distance configurations to inherit"""

    @abstractmethod
    def compute_distances(self, pairs):
        return

    # @abstractmethod
    # def config(self):
    #     return


class BaseCluster(metaclass=ABCMeta):
    """Abstract base class for all clustering algos to inherit"""

    @abstractmethod
    def get_df_cluster(self):
        return
