from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import List, Optional, Tuple

from oagdedupe import utils as du
from oagdedupe._typing import ENGINE, StatsDict
from oagdedupe.block import base as block
from oagdedupe.settings import Settings


# db
@dataclass
class BaseComputeBlocking(ABC):
    def max_key(self, x: StatsDict) -> Tuple[float, int, int]:
        """
        block scheme stats ordering
        """
        return (x.rr, x.positives, -x.negatives)

    @abstractmethod
    @du.recordlinkage_repeat
    def build_forward_indices(
        self,
        full: bool = False,
        rl: str = "",
        engine: Optional[ENGINE] = None,
        iter: Optional[int] = None,
        columns: Optional[Tuple[str]] = None,
    ) -> None:
        pass

    @abstractmethod
    @du.recordlinkage
    def get_inverted_index_stats(
        self, names: Tuple[str], table: str, rl: str = ""
    ) -> StatsDict:
        pass

    @abstractmethod
    @du.recordlinkage
    def save_comparison_pairs(
        self, names: Tuple[str], table: str, rl: str = ""
    ) -> None:
        pass

    @abstractmethod
    def get_n_pairs(self, table: str):
        pass


@dataclass
class BaseCompute(ABC):
    @abstractmethod
    def setup(
        self, df=None, df2=None, reset=True, resample=False, rl: str = ""
    ) -> None:
        """sets up environment

        creates:
        - df
        - pos
        - neg
        - unlabelled
        - train
        - labels
        """
        pass

    @abstractmethod
    def label_distances(self):
        """computes distances for labels"""
        pass

    @abstractmethod
    def get_scores(self, threshold):
        """returns model predictions"""
        pass

    @abstractmethod
    def merge_clusters_with_raw_data(self, df_clusters, rl):
        """appends attributes to predictions"""
        pass

    @abstractmethod
    def save_comparison_attributes_dists(self, full, labels):
        """computes distances on attributes"""
        pass


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
    def save_distances(self, full: bool):
        return


# cluster
class BaseCluster(ABC):
    """Abstract base class for all clustering algos to inherit"""

    @abstractmethod
    def get_df_cluster(self):
        return
