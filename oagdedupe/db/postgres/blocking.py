"""This module contains mixins with common methods in oagdedupe.block
"""

import logging
from dataclasses import dataclass
from functools import cached_property
from multiprocessing import Pool
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

import oagdedupe.utils as du
from oagdedupe._typing import ENGINE, StatsDict
from oagdedupe.block.schemes import BlockSchemes
from oagdedupe.db.base import BaseRepositoryBlocking
from oagdedupe.settings import Settings


@dataclass
class BlockingMixin:
    """convenience funcs particular to postgres"""

    def query_blocks(self, table: str, columns: List[str]) -> str:
        """
        Builds SQL query used to build forward indices.

        Parameters
        ----------
        table : str
        columns : List[str]

        Returns
        ----------
        str
        """
        return f"""
            DROP TABLE IF EXISTS {self.settings.db.db_schema}.blocks_{table};

            CREATE TABLE {self.settings.db.db_schema}.blocks_{table} as (
                SELECT
                    _index,
                    {", ".join(columns)}
                FROM {self.settings.db.db_schema}.{table}
            );
        """

    def add_scheme(
        self,
        table: str,
        col: str,
        exists: List[str],
        rl: str = "",
    ) -> None:
        """
        check if column is in exists
        if not, add to blocks_{tablle}
        """

        if col in exists:
            return

        if "ngrams" in col:
            coltype = "text[]"
        else:
            coltype = "text"

        self.execute(
            f"""
            ALTER TABLE {self.settings.db.db_schema}.blocks_{table}{rl}
            ADD COLUMN IF NOT EXISTS {col} {coltype}
        """
        )

        self.execute(
            f"""
            UPDATE {self.settings.db.db_schema}.blocks_{table}{rl} AS t1
            SET {col} = t2.{col}
            FROM (
                SELECT _index, {self.block_scheme_mapping[col]} as {col}
                FROM {self.settings.db.db_schema}.{table}{rl}
            ) t2
            WHERE t1._index = t2._index;
        """
        )
        return

    @du.recordlinkage_repeat
    def init_forward_index_full(self, rl: str = "") -> None:
        """initialize full index table"""
        self.execute(
            f"""
            DROP TABLE IF EXISTS {self.settings.db.db_schema}.blocks_df{rl};

            CREATE TABLE {self.settings.db.db_schema}.blocks_df{rl} as (
                SELECT
                    _index
                FROM {self.settings.db.db_schema}.df{rl}
            );
        """
        )

    def query(self, sql: str) -> pd.DataFrame:
        """
        for parallel implementation, need to create separate engine
        for each process
        """
        engine = create_engine(self.settings.db.path_database)
        res = pd.read_sql(sql, con=engine)
        engine.dispose()
        return res

    def execute(self, sql: str) -> None:
        """
        for parallel implementation, need to create separate engine
        for each process
        """
        engine = create_engine(self.settings.db.path_database)
        engine.execute(sql)
        engine.dispose()

    @du.recordlinkage_both
    def n_df(self, rl: str = "") -> pd.DataFrame:
        return self.query(
            f"SELECT count(*) FROM {self.settings.db.db_schema}.df{rl}"
        )["count"].values[0]

    @cached_property
    def n_comparisons(self) -> float:
        """number of total possible comparisons"""
        n = self.n_df()
        if not self.settings.model.dedupe:
            return np.product(n)
        return (n * (n - 1)) / 2

    @property
    def min_rr(self) -> float:
        """minimum reduction ratio"""
        reduced = self.n_comparisons - self.settings.model.max_compare
        return reduced / self.n_comparisons

    @du.recordlinkage
    def _pairs_query(self, names: Tuple[str], rl: str = "") -> str:
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

    def check_unnest(self, name):
        if "ngrams" in name:
            return f"unnest({name})"
        return name

    def signatures(self, names):
        return ", ".join(
            [
                f"{self.check_unnest(name)} as signature{i}"
                for i, name in enumerate(names)
            ]
        )

    def _aliases(self, names: Tuple[str]) -> List[str]:
        return [f"signature{i}" for i in range(len(names))]

    def _inv_idx_query(
        self, names: Tuple[str], table: str, col: str = "_index_l"
    ) -> str:
        return f"""
        SELECT
            {self.signatures(names)},
            unnest(ARRAY_AGG(_index ORDER BY _index asc)) {col}
        FROM {self.settings.db.db_schema}.{table}
        GROUP BY {", ".join(self._aliases(names))}
        """

    @property
    def comptab_map(self) -> Dict[str, str]:
        return {"blocks_train": "comparisons", "blocks_df": "full_comparisons"}


@dataclass
class PostgresBlocking(BaseRepositoryBlocking, BlockingMixin, BlockSchemes):
    settings: Settings

    @du.recordlinkage_repeat
    def build_forward_indices(
        self,
        full: bool = False,
        rl: str = "",
        iter: Optional[int] = None,
        columns: Optional[Tuple[str]] = None,
    ) -> None:
        """
        Executes SQL queries to build forward indices on train or full data

        Parameters
        ----------
        columns : List[str]
            block schemes to include in forward index
        """
        if full:
            if iter == 0:
                self.init_forward_index_full()
            for col in columns:

                logging.info(
                    "building forward index on full data for scheme %s", col
                )

                exists = self.query(
                    f"SELECT * FROM {self.settings.db.db_schema}.blocks_df LIMIT 1"
                ).columns

                self.add_scheme(table="df", col=col, exists=exists, rl=rl)
        else:
            self.execute(
                self.query_blocks(
                    table=f"train{rl}", columns=self.block_scheme_sql
                )
            )

    @du.recordlinkage
    def get_inverted_index_stats(
        self, names: Tuple[str], table: str, rl: str = ""
    ) -> StatsDict:
        """
        Given forward index, construct inverted index.
        Then for each row in inverted index, get all "nC2" distinct
        combinations of size 2 from the array.

        Then compute number of pairs, the positive coverage and negative
        coverage.

        Parameters
        ----------
        names : List[str]
            list of block schemes
        table : str
            table name of forward index

        Returns
        ----------
        StatsDict
        """
        res = (
            self.query(
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
                    FROM {self.settings.db.db_schema}.labels
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
            )
            .fillna(0)
            .loc[0]
            .to_dict()
        )

        res["scheme"] = names
        res["rr"] = 1 - (res["n_pairs"] / (self.n_comparisons))

        return StatsDict(**res)

    def get_n_pairs(self, table: str) -> pd.DataFrame:
        newtable = self.comptab_map[table]
        return self.query(
            f"""
            SELECT count(*) FROM {self.settings.db.db_schema}.{newtable}
        """
        )["count"].values[0]

    @du.recordlinkage
    def save_comparison_pairs(
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
