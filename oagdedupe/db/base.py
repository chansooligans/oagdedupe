import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import List, Optional, Tuple

import numpy as np
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

    settings: Settings

    @abstractmethod
    @du.recordlinkage_repeat
    def resample(self) -> None:
        """Used by fast-api to generate new samples between active learning
        cycles.

        The `train` table contains a column, `labels` indicating whether the
        sample exists in the labels table. Delete all records where
        labels == False.

        Then append a new sample of size n to the `train` table.

        Importantly,train records that WERE labelled should not be discarded.

        Example:
        If there are 20 records with labels == True, there should be n + 20
        records in `train` after resampling.
        """
        pass

    @abstractmethod
    @du.recordlinkage
    def setup(self, df=None, df2=None, rl: str = "") -> None:
        """sets up environment

        Creates the following tables (see oagdedupe/db/postgres/tables for an
        example of table schemas in sqlalchemy):
        - df
        - train (which consists of pos/neg/unlabelled)
            - pos
            - neg
            - unlabelled
        - labels
        - blocks_df (sql only)

        if record linakge, also create:
        - df_link
        - neg_link
        - unlabelled_link
        - train_link

        if SQL, also create blocking scheme functions
        e.g. funcs.create_functions(settings=self.settings)

        Parameters
        ----------
        df: pd.DataFrame
            dataframe to dedupe
        df2: Optional[pd.DataFrame]
            dataframe for recordlinkage
        rl: str
            for recordlinkage, used by decorator

        Returns
        ----------
        None

        saves each table in database/memory
        """

        pass


@dataclass
class BaseRepositoryBlocking(ABC, BlockSchemes):
    """abstract implementation for blocking-related operations"""

    settings: Settings

    def max_key(self, x: StatsDict) -> Tuple[float, int, int]:
        """
        block scheme stats ordering

        Parameters
        ----------
        x: StatsDict

        Returns
        ----------
        Tuple(float, int, int): reduction ratio, positive coverage, negative coverage
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

        If full == False, construct forward index on `train` table
        for all block schemes and save output to `blocks_train`

        If full == True, for each conjunction (from best to worst),
        construct forward index for a scheme at a time,
        using "add_scheme()" and append output to `blocks_df`. If scheme already
        exists in `blocks_df`, skip!

        Parameters
        ----------
        full: bool = False
            indicator for wheter to use `train` or `df`
        rl: str
            for recordlinkage, used by decorator
        iter: Optional[int]
            ?
        conjunction: Optional[Tuple[str]]
            dataframe for recordlinkage(?)

        Returns
        ----------
        in sql, save to `blocks_train`/`blocks_train_link` or `blocks_df`/`blocks_df_link`
        """
        pass

    @abstractmethod
    def add_scheme(
        self,
        scheme: str,
        rl: str = "",
    ) -> None:
        """Only used for building forward index on full data;

        Check if scheme already exists in forward index; if not, add to
        blocks_df

        Parameters
        ----------
        scheme: str
            name of scheme
        exists: bool
            whether scheme already exists in blocks_df
        rl: str
            for recordlinkage, used by decorator

        Returns
        ----------
        in sql, appends to `blocks_df`/`blocks_df_link`
        """

    @abstractmethod
    def build_inverted_index(
        self, conjunction: Tuple[str], table: str, col: str = "_index_l"
    ) -> None:
        """Gets "exploded" inverted index for a particular conjunction, from either
        blocks_train (for sample) or blocks_df (for full).

        If either of the schemes returned an array (e.g. 3-grams), unnest/explode
        the array. If none of the schemes returned an array, it's equivalent
        to the forward index itself.

        Example with a conjunction containing two schemes:

        | first_letter_first_name | 3-grams | _index |
        |-------------------------|---------|--------|
        | J                       | "Jac"   | 0      |
        | J                       | "ack"   | 0      |
        | J                       | "ck "   | 0      |
        | D                       | "Dap"   | 1      |
        | D                       | "aph"   | 1      |

        Parameters
        ----------
        conjunction: Tuple[str]
            tuple of block schemes
        table: str
            either "blocks_train" or "blocks_df"
        col: str
            only for SQL

        Returns
        ----------
        in sql, returns a query
        """
        pass

    @abstractmethod
    @du.recordlinkage
    def pairs_query(
        self, conjunction: Tuple[str], rl: str = ""
    ) -> pd.DataFrame:
        """Get comparison pairs for a conjunction.

        For dedupe, cross join the "exploded" inverted index from
        build_inverted_index() to itself. Keep only if _index_l < index_r.

        For record linkage, join "exploded" inverted index from df1 to a
        second one from df2, keeping all pairs.

        Return a dataframe with two columns (_index_l and _index_r) and
        duplicates removed.

        Parameters
        ----------
        conjunction: Tuple[str]
            tuple of block schemes
        rl: str
            for recordlinkage, used by decorator

        Returns
        ----------
        in sql, returns a query
        """
        pass

    @abstractmethod
    @du.recordlinkage
    def get_conjunction_stats(
        self, conjunction: Tuple[str], table: str, rl: str = ""
    ) -> StatsDict:
        """Get stats for a conjunction, where stats include:
        - reduction ratio
        - positive coverage
        - negative coverage
        - the number of pairs
        - name of scheme

        Using comparison pairs from pairs_query(), left join to the labels
        table. Positive coverage is the sum of the positive labels and
        negative coverage is the sum of the negative labels.

        Reduction ratio is the nubmer of pairs divided by the total
        number of comparisons (for dedupe, (n*(n-1)) / 2; for record linkage,
        n1 * n2)

        Parameters
        ----------
        conjunction: Tuple[str]
            tuple of block schemes
        table: str
            either "blocks_train" or "blocks_df"
        rl: str
            for recordlinkage, used by decorator

        Returns
        ----------
        StatsDict
        """
        pass

    @abstractmethod
    @du.recordlinkage
    def add_new_comparisons(
        self, conjunction: Tuple[str], table: str, rl: str = ""
    ) -> None:
        """Appends comparison pairs from pairs_query() into a new table,
        either "comparisons" or "full_comparisons". See mapping from
        table to new_table below:

        {
            "blocks_train": "comparisons",
            "blocks_df": "full_comparisons"
        }

        Parameters
        ----------
        conjunction: Tuple[str]
            tuple of block schemes
        table: str
            either "blocks_train" or "blocks_df"
        rl: str
            for recordlinkage, used by decorator

        Returns
        ----------
        in sql, saves to either "comparisons" or "full_comparisons"
        """
        pass

    @abstractmethod
    def get_n_pairs(self, table: str) -> int:
        """Gets number of pairs collected in comparisons or full_comparisons

        if table == "blocks_train", use comparisons;
        elif table == "blocks_df", use full_comparisons;

        This function is used when applying list of best conjunctions to
        get comparison_pairs. We want to log the number of pairs to know
        when to stop.

        For "blocks_train", stopping rule is hard coded into
        oagdedupe.block.blocking.save(). For "blocks_full", stopping rule is
        stored in SettingsModel.n_covered and can be modified by user.

        Parameters
        ----------
        table: str
            either "blocks_train" or "blocks_df"

        Returns
        ----------
        int

        """
        pass


@dataclass
class BaseDistanceRepository(ABC):
    @abstractmethod
    def compute_distances(self) -> None:
        """computes distances on attributes

        currently only uses jarowinkler, but will be changed later on
        to accommodate for different string distance metrics and also
        non-str types

        Returns
        ----------
        in sql, simply returns a subquery
        """
        pass

    @abstractmethod
    def save_distances(self, full: bool, labels: bool) -> None:
        """saves computed distances

        - if labels is True, compute distances on `labels` table and save
        to `labels_distances` table
        - elif full is True, compute distances on `full_comparisons` table
        and save to `full_distances` table
        - else compute distances on `comparisons` table and save to
        `distances` table

        optionally, distances for labels can be appended to the labels table
        itself

        Returns
        ----------
        in sql, saves to 'labels_distances', 'full_distances', or 'distances'
        """
        pass


@dataclass
class BaseFapiRepository(ABC):
    def predict(self, dists: np.array):
        """Submits post request to FastAPI using predict() to get
        predicted probabilities of match

        Parameters
        ----------
        dists: np.array

        Returns
        ----------
        List[list] predictions
        """
        return json.loads(
            requests.post(
                f"{self.settings.fast_api.url}/predict",
                json={"dists": dists.tolist()},
            ).content
        )

    @abstractmethod
    def save_predictions(self):
        """gets `full_distances` table and posts distances to FastAPI to get
        predicted probabilities of match

        saves output to the "scores" table, which has three columns
        (_index_l, _index_r, score)
        """
        pass

    @abstractmethod
    def update_train(self, newlabels: pd.DataFrame) -> None:
        """
        From FastAPI, this method is used at end of each active learning loop;
        For entities that were labelled, set "labelled" column in
        `train` table to True
        """
        pass

    @abstractmethod
    def update_labels(self, newlabels: pd.DataFrame) -> None:
        """
        From FastAPI, this method is used at end of each active learning loop;
        For entities that were labelled, add these newly labelled records
        to the `labels` table.
        """
        pass

    @abstractmethod
    def get_distances(self) -> pd.DataFrame:
        """Get the distances table to obtain uncertainty samples.

        This query should get the distances and join to `labels_distances`
        on _index_l and _index_r. The purpose of this is to get the raw
        attribute columns since we want to pass these to LabelStudio for
        human reviewer to assess.
        """
        pass

    @abstractmethod
    def get_labels(self) -> pd.DataFrame:
        """Get the labels_distances table (or labels) table;

        This query is used by FastAPI to teach the active learner. So it should
        have the labels and their distances.
        """
        pass


@dataclass
class BaseClusterRepository(ABC):
    @abstractmethod
    def get_scores(self, threshold) -> pd.DataFrame:
        """Get the `scores` table, created in
        BaseFapiRepository.save_predictions()

        Discard records whose scores are below threshold.
        """
        pass

    @abstractmethod
    def merge_clusters_with_raw_data(self, df_clusters, rl):
        """wrapper for get_clusters() and get_clusters_link(); call
        get_clusters_link() instead of get_clusters() if rl != ""

        in sql, also saves df_clusters to `clusters` table

        Parameters
        ----------
        `df_clusters` is the output of get_connected_components() in
        oagdedupe.cluster
        """
        pass

    @abstractmethod
    def get_clusters(self) -> pd.DataFrame:
        """adds cluster IDs to df and returns dataframe"""
        pass

    @abstractmethod
    def get_clusters_link(self, threshold) -> List[pd.DataFrame]:
        """adds cluster IDs to df and df_link and retursn list of dataframes"""
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
