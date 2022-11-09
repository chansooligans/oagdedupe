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

    @property
    def comptab_map(self) -> Dict[str, str]:
        return {"blocks_train": "comparisons", "blocks_df": "full_comparisons"}


@dataclass
class PostgresBlockingRepository(
    BaseRepositoryBlocking, BlockingMixin, BlockSchemes
):
    settings: Settings

    @du.recordlinkage_repeat
    def build_forward_indices(
        self,
        full: bool = False,
        rl: str = "",
        conjunction: Optional[Tuple[str]] = None,
    ) -> None:
        """
        Executes SQL queries to build forward indices on train or full data

        Parameters
        ----------
        schemes : List[str]
            block schemes to include in forward index
        """
        if full:

            columns = self.query(
                f"SELECT * FROM {self.settings.db.db_schema}.blocks_df LIMIT 1"
            ).columns

            for scheme in conjunction:

                logging.info(
                    "building forward index on full data for scheme %s", scheme
                )

                if scheme not in columns:
                    self.add_scheme(scheme=scheme, rl=rl)
        else:
            self.execute(
                self.query_blocks(
                    table=f"train{rl}", columns=self.block_scheme_sql
                )
            )

    def add_scheme(
        self,
        scheme: str,
        rl: str = "",
    ) -> None:
        """
        only used for building blocks_train on full data;

        check if column is in exists
        if not, add to blocks_df
        """

        if "ngrams" in scheme:
            coltype = "text[]"
        else:
            coltype = "text"

        self.execute(
            f"""
            ALTER TABLE {self.settings.db.db_schema}.blocks_df{rl}
            ADD COLUMN IF NOT EXISTS {scheme} {coltype}
        """
        )

        self.execute(
            f"""
            UPDATE {self.settings.db.db_schema}.blocks_df{rl} AS t1
            SET {scheme} = t2.{scheme}
            FROM (
                SELECT _index, {self.block_scheme_mapping[scheme]} as {scheme}
                FROM {self.settings.db.db_schema}.df{rl}
            ) t2
            WHERE t1._index = t2._index;
        """
        )
        return

    def build_inverted_index(
        self, conjunction: Tuple[str], table: str, col: str = "_index_l"
    ) -> str:
        return f"""
        SELECT
            {self.signatures(conjunction)}, _index {col}
        FROM {self.settings.db.db_schema}.{table}
        """

    @du.recordlinkage
    def get_conjunction_stats(
        self, conjunction: Tuple[str], table: str, rl: str = ""
    ) -> StatsDict:
        """
        Given forward index, construct inverted index.
        Then for each row in inverted index, get all "nC2" distinct
        combinations of size 2 from the array.

        Then compute number of pairs, the positive coverage and negative
        coverage.

        Parameters
        ----------
        conjunction : List[str]
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
                    {self.build_inverted_index(conjunction, table)}
                ),
                inverted_index_link AS (
                    {self.build_inverted_index(conjunction, table+rl, col="_index_r")}
                ),
                pairs AS (
                    {self.pairs_query(conjunction)}
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

        res["conjunction"] = conjunction
        res["rr"] = 1 - (res["n_pairs"] / (self.n_comparisons))

        return StatsDict(**res)

    @du.recordlinkage
    def pairs_query(self, conjunction: Tuple[str], rl: str = "") -> str:
        if rl == "":
            where = "WHERE t1._index_l < t2._index_r"
        else:
            where = ""
        return f"""
            SELECT _index_l, _index_r
            FROM inverted_index t1
            JOIN inverted_index_link t2
                ON {" and ".join(
                    [f"t1.{s} = t2.{s}" for s in self._aliases(conjunction)]
                )}
            {where}
            GROUP BY _index_l, _index_r
            """

    @du.recordlinkage
    def add_new_comparisons(
        self, conjunction: Tuple[str], table: str, rl: str = ""
    ) -> None:
        """
        Given forward index, construct inverted index.
        Then for each row in inverted index, get all "nC2" distinct
        combinations of size 2 from the array.

        Concatenates and returns all distinct pairs.

        Parameters
        ----------
        conjunction : List[str]
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
            INSERT INTO {self.settings.db.db_schema}.{newtable} (_index_l, _index_r)
            (
                WITH
                    inverted_index AS (
                        {self.build_inverted_index(conjunction, table)}
                    ),
                    inverted_index_link AS (
                        {self.build_inverted_index(conjunction, table+rl, col="_index_r")}
                    )
                {self.pairs_query(conjunction)}
            )
            ON CONFLICT DO NOTHING
            """
        )
        engine.dispose()

    def get_n_pairs(self, table: str) -> int:
        newtable = self.comptab_map[table]
        return self.query(
            f"""
            SELECT count(*) FROM {self.settings.db.db_schema}.{newtable}
        """
        )["count"].values[0]
