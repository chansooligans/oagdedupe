"""Contains object to query and manipulate data from postgres 
to construct inverted index and comparison pairs.

This module is only used by oagdedupe.block.learner
"""

from dataclasses import dataclass
from functools import cached_property

import numpy as np
import pandas as pd
from typing import Tuple
from sqlalchemy import create_engine

from oagdedupe import utils as du
from oagdedupe.settings import Settings


def check_unnest(name):
    if "ngrams" in name:
        return f"unnest({name})"
    return name


def signatures(names):
    return ", ".join(
        [
            f"{check_unnest(name)} as signature{i}"
            for i, name in enumerate(names)
        ]
    )

@dataclass
class StatsDict:
    n_pairs: int
    positives: int
    negatives: int
    scheme: Tuple[str]
    rr: float


@dataclass
class LearnerSql:
    """
    Object contains methods to query database using sqlalchemy CORE;
    It's easier to use sqlalchemy core than ORM for parallel operations.
    """

    settings: Settings

    def query(self, sql):
        """
        for parallel implementation, need to create separate engine
        for each process
        """
        engine = create_engine(self.settings.other.path_database)
        res = pd.read_sql(sql, con=engine)
        engine.dispose()
        return res

    def truncate_table(self, table):
        engine = create_engine(self.settings.other.path_database)
        engine.execute(
            f"""
            TRUNCATE TABLE {self.settings.other.db_schema}.{table};
        """
        )
        engine.dispose()

    @property
    def comptab_map(self):
        return {"blocks_train": "comparisons", "blocks_df": "full_comparisons"}

    def _aliases(self, names):
        return [f"signature{i}" for i in range(len(names))]

    def _inv_idx_query(self, names, table, col="_index_l"):
        return f"""
        SELECT 
            {signatures(names)}, 
            unnest(ARRAY_AGG(_index ORDER BY _index asc)) {col}
        FROM {self.settings.other.db_schema}.{table}
        GROUP BY {", ".join(self._aliases(names))}
        """

    @du.recordlinkage
    def _pairs_query(self, names, rl=""):
        if rl == "":
            where = "WHERE t1._index_l < t2._index_r"
        else:
            where = ""
        return f"""
            SELECT _index_l, _index_r
            FROM inverted_index t1
            JOIN inverted_index_link t2
                ON {" and ".join(
                    [f"t1.{s} = t2.{s}" for s in self._aliases(names)]
                )}
            {where}
            GROUP BY _index_l, _index_r
            """

    @du.recordlinkage
    def save_comparison_pairs(self, names, table, rl=""):
        """
        see dedupe.block.learner.InvertedIndex;

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
        engine = create_engine(self.settings.other.path_database)
        engine.execute(
            f"""
            INSERT INTO {self.settings.other.db_schema}.{newtable}
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

    def get_n_pairs(self, table):
        newtable = self.comptab_map[table]
        return self.query(
            f"""
            SELECT count(*) FROM {self.settings.other.db_schema}.{newtable}
        """
        )["count"].values[0]

    @du.recordlinkage
    def get_inverted_index_stats(self, names, table, rl=""):
        """
        Given forward index, construct inverted index.
        Then for each row in inverted index, get all "nC2" distinct
        combinations of size 2 from the array. Then compute
        number of pairs, the positive coverage and negative coverage.

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
        res = self.query(
            f"""
            WITH 
                inverted_index AS (
                    {self._inv_idx_query(names, table)}
                ),
                inverted_index_link AS (
                    {self._inv_idx_query(names, table+rl, col="_index_r")}
                ),
                pairs AS (
                    {self._pairs_query(names)}
                ),
                labels AS (
                    SELECT _index_l, _index_r, label
                    FROM {self.settings.other.db_schema}.labels
                )
            SELECT 
                count(*) as n_pairs,
                SUM(CASE WHEN t2.label = 1 THEN 1 ELSE 0 END) positives,
                SUM(CASE WHEN t2.label = 0 THEN 1 ELSE 0 END) negatives
            FROM pairs t1
            LEFT JOIN labels t2
                ON t2._index_l = t1._index_l
                AND t2._index_r = t1._index_r
            """
        ).fillna(0).loc[0].to_dict()

        res["scheme"] = names
        res["rr"] = 1 - (res["n_pairs"] / (self.n_comparisons))

        return StatsDict(**res)

    @cached_property
    def blocking_schemes(self):
        """
        Get all blocking schemes

        Returns
        ----------
        List[str]
        """
        return [
            tuple([x])
            for x in self.query(
                f"""
                SELECT * 
                FROM {self.settings.other.db_schema}.blocks_train LIMIT 1
                """
            )
            .columns[1:]
            .tolist()
        ]

    @du.recordlinkage_both
    def n_df(self, rl=""):
        return self.query(
            f"SELECT count(*) FROM {self.settings.other.db_schema}.df{rl}"
        )["count"].values[0]

    @cached_property
    def n_comparisons(self):
        """number of total possible comparisons"""
        n = self.n_df()
        if self.settings.other.dedupe == False:
            return np.product(n)
        return (n * (n - 1)) / 2

    @property
    def min_rr(self):
        """minimum reduction ratio"""
        reduced = self.n_comparisons - self.settings.other.max_compare
        return reduced / self.n_comparisons
