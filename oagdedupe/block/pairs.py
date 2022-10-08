"""Contains object to query and manipulate data from postgres
to construct inverted index and comparison pairs.

This module is only used by oagdedupe.block.learner
"""

from dataclasses import dataclass
from functools import cached_property
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from dependency_injector.wiring import Provide
from sqlalchemy import create_engine

from oagdedupe import utils as du
from oagdedupe._typing import ENGINE, StatsDict
from oagdedupe.block.mixin import ConjunctionMixin
from oagdedupe.containers import Container
from oagdedupe.settings import Settings


@dataclass
class Pairs(ConjunctionMixin):
    """
    Computes pairs for conjunctions and appends to comparisons or
    full_comparisons table.
    """

    settings: Settings = Provide[Container.settings]

    def _get_n_pairs(self, table: str) -> pd.DataFrame:
        newtable = self.comptab_map[table]
        return self.query(
            f"""
            SELECT count(*) FROM {self.settings.db.db_schema}.{newtable}
        """
        )["count"].values[0]

    @du.recordlinkage
    def _save_comparison_pairs(
        self, names: Tuple[str], table: str, rl: str = ""
    ) -> None:
        """
        Given forward index, construct inverted index.
        Then for each row in inverted index, get all "nC2" distinct
        combinations of size 2 from the array.

        Concatenates and returns all distinct pairs.

        Parameters
        ----------
        names : List[str]
            list of block schemes
        table : str
            table name of forward index

        Returns
        ----------
        pd.DataFrame
        """
        newtable = self.comptab_map[table]
        engine = create_engine(self.settings.db.path_database)
        engine.execute(
            f"""
            INSERT INTO {self.settings.db.db_schema}.{newtable}
            (
                WITH
                    inverted_index AS (
                        {self._inv_idx_query(names, table)}
                    ),
                    inverted_index_link AS (
                        {self._inv_idx_query(names, table+rl, col="_index_r")}
                    )
                {self._pairs_query(names)}
            )
            ON CONFLICT DO NOTHING
            """
        )
        engine.dispose()

    def add_new_comparisons(
        self, stats: StatsDict, table: str, engine: ENGINE
    ) -> int:
        """
        Computes pairs for conjunction and appends to comparisons or
        full_comparisons table.

        When training on sample, forward indices are pre-computed;
        But for full data, forward indices construction can be expensive,
        so they are computed here as needed.

        Parameters
        ----------
        df: pd.DataFrame
            comparisons pairs already gathered
        stats: dict
            stats for new block scheme
        table: str
            table used to get pairs (either blocks_train for sample or
            blocks_df for full df)

        Returns
        ----------
        int
            total number of pairs gathered so far
        """

        self._save_comparison_pairs(names=stats.scheme, table=table)
        n = self._get_n_pairs(table=table)
        return n
