import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import List, Optional, Tuple

import pandas as pd
import requests

from oagdedupe import utils as du
from oagdedupe._typing import ENGINE, StatsDict
from oagdedupe.block.schemes import BlockSchemes
from oagdedupe.settings import Settings


@dataclass
class BaseInitializeRepository(ABC):
    """Abstract implementation for initialization operations

    This repository only requires setup(), which is called at initialization

    """

    @abstractmethod
    @du.recordlinkage_repeat
    def resample(self) -> None:
        """Used by fast-api to generate new samples between active learning
        cycles.

        The `train` table contains a column, `labels` indicating whether the
        sample exists in the labels table. Delete all records where
        labels == False. Then replace with a new sample of size n. Importantly,
        train records that WERE labelled should not be discarded. If there are
        20 records with labels == True, there should be n + 20 records
        after resampling.
        """
        pass

    @abstractmethod
    @du.recordlinkage
    def setup(self, df=None, df2=None, rl: str = "") -> None:
        """sets up environment

        Parameters
        ----------
        df: Optional[pd.DataFrame]
            dataframe to dedupe
        df2: Optional[pd.DataFrame]
            dataframe for recordlinkage
        rl: str
            for recordlinkage, used by decorator

        Creates the following tables (see oagdedupe/db/postgres/tables for an
        example of table schemas in sqlalchemy):
        - df
        - pos
        - neg
        - unlabelled
        - train
        - labels

        if record linakge also create:
        - df_link
        - neg_link
        - unlabelled_link
        - train_link
        """
        # if SQL, create blocking scheme functions
        # funcs.create_functions(settings=self.settings)
        pass


@dataclass
class BaseRepositoryBlocking(ABC, BlockSchemes):
    """abstract implementation for blocking-related operations"""

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
        iter: Optional[int] = None,
        conjunction: Optional[Tuple[str]] = None,
    ) -> None:
        """Builds forward indices on train or full data

        A forward index is a mapping from entity to signature. As an example,
        for a table with 2 block schemes:

        | Entity        | first_letter_first_name | 3-grams    |
        |---------------|-------------------------|------------|
        | Jack Johnson  | J                       |['Jac', 'ack', 'ck ', 'k J', ' Jo', 'Joh', 'ohn', 'hns', 'nso', 'son']|
        | Elvis Presley | E                       |['Elv', 'lvi', 'vis', 'is ', 's P', ' Pr', 'Pre', 'res', 'esl', 'sle', 'ley']|
        | Tom Waits     | T                       |['Tom', 'om ', 'm W', ' Wa', 'Wai', 'ait', 'its']|
        | Dolly Parton  | D                       |['Dol', 'oll', 'lly', 'ly ', 'y P', ' Pa', 'Par', 'art', 'rto', 'ton']|
        | Brenda Lee    | B                       |['Bre', 'ren', 'end', 'nda', 'da ', 'a L', ' Le', 'Lee']|

        If full == False, construct forward index on `train` table and
        save output to blocks_train.

        If full == True, construct forward index for a conjunction at a time.
        """
        pass

    def build_inverted_index(
        self, names: Tuple[str], table: str, col: str = "_index_l"
    ) -> str:
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

    @du.recordlinkage
    def pairs_query(self, names: Tuple[str], rl: str = "") -> str:
        pass

    @abstractmethod
    @du.recordlinkage
    def add_new_comparisons(
        self, names: Tuple[str], table: str, rl: str = ""
    ) -> None:
        """apply inverted index to get comparison pairs"""
        pass

    @abstractmethod
    def get_n_pairs(self, table: str):
        pass


@dataclass
class BaseDistanceRepository(ABC):
    @abstractmethod
    def get_distances(self) -> pd.DataFrame:
        """get the labels table"""
        pass

    @abstractmethod
    def compute_distances(self) -> pd.DataFrame:
        """get the labels table"""
        pass

    @abstractmethod
    def save_distances(self, full, labels):
        """computes distances on attributes"""
        pass


@dataclass
class BaseClusterRepository(ABC):
    @abstractmethod
    def get_scores(self, threshold):
        """returns model predictions"""
        pass

    @abstractmethod
    def get_clusters(self, threshold):
        """returns model predictions"""
        pass

    @abstractmethod
    def get_clusters_link(self, threshold):
        """returns model predictions"""
        pass

    @abstractmethod
    def merge_clusters_with_raw_data(self, df_clusters, rl):
        """appends attributes to predictions"""
        pass


@dataclass
class BaseFapiRepository(ABC):
    def predict(self, dists):
        return json.loads(
            requests.post(
                f"{self.settings.fast_api.url}/predict",
                json={"dists": dists.tolist()},
            ).content
        )

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
    def get_labels(self) -> pd.DataFrame:
        """get the labels table"""
        pass

    @abstractmethod
    def save_predictions(self):
        """
        submits post request to FastAPI to get predicted labels using
        active learner model;
        """
        pass


@dataclass
class BaseRepository(
    BaseInitializeRepository,
    BaseDistanceRepository,
    BaseClusterRepository,
    BaseFapiRepository,
    ABC,
):
    """abstract implementation for compute"""

    settings: Settings

    @cached_property
    @abstractmethod
    def blocking(self):
        return BaseRepositoryBlocking(settings=self.settings)
