from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import List, Optional, Tuple

import pandas as pd

from oagdedupe import utils as du
from oagdedupe._typing import ENGINE, StatsDict
from oagdedupe.block.schemes import BlockSchemes
from oagdedupe.settings import Settings


@dataclass
class BaseComputeBlocking(ABC, BlockSchemes):
    settings: Settings

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
        """build forward indices on train or full data"""
        pass

    @abstractmethod
    @du.recordlinkage
    def get_inverted_index_stats(
        self, names: Tuple[str], table: str, rl: str = ""
    ) -> StatsDict:
        """get inverted index stats:
        - reduction ratio
        - positive coverage
        - negative coverage
        - the number of pairs
        - name of scheme
        """
        pass

    @abstractmethod
    @du.recordlinkage
    def save_comparison_pairs(
        self, names: Tuple[str], table: str, rl: str = ""
    ) -> None:
        """apply inverted index to get comparison pairs"""
        pass

    @abstractmethod
    def get_n_pairs(self, table: str):
        pass


@dataclass
class BaseCompute(ABC):
    settings: Settings

    @cached_property
    @abstractmethod
    def blocking(self):
        return BaseComputeBlocking(settings=self.settings)

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
    def save_distances(self, full, labels):
        """computes distances on attributes"""
        pass

    @abstractmethod
    def merge_clusters_with_raw_data(self, df_clusters, rl):
        """appends attributes to predictions"""
        pass

    @abstractmethod
    def update_train(self, newlabels: pd.DataFrame) -> None:
        """
        for entities that were labelled,
        set "labelled" column in train table to True
        """
        pass

    @abstractmethod
    def update_labels(self, newlabels: pd.DataFrame) -> None:
        """
        add new labels to labels table
        """
        pass

    @abstractmethod
    def predict(self):
        """
        use active learner model to predict whether comparison pairs
        are matches
        """
        pass

    @abstractmethod
    def get_scores(self, threshold):
        """returns model predictions"""
        pass

    @abstractmethod
    def get_distances(self) -> pd.DataFrame:
        """get the labels table"""
        pass

    @abstractmethod
    def get_labels(self) -> pd.DataFrame:
        """get the labels table"""
        pass
