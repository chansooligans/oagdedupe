"""This module contains objects used to construct blocks by
creating forward index.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
from dependency_injector.wiring import Provide

from oagdedupe import utils as du
from oagdedupe._typing import ENGINE
from oagdedupe.block.base import BaseForward
from oagdedupe.block.schemes import BlockSchemes
from oagdedupe.containers import Container
from oagdedupe.settings import Settings


@dataclass
class Forward(BlockSchemes, BaseForward):
    """
    Used to build forward indices. A forward index
    is a table where rows are entities, columns are block schemes,
    and values contain signatures.

    Attributes
    ----------
    settings : Settings
    """

    settings: Settings = Provide[Container.settings]

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

    @du.recordlinkage_repeat
    def add_scheme(
        self,
        table: str,
        col: str,
        exists: List[str],
        engine: ENGINE,
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

        engine.execute(
            f"""
            ALTER TABLE {self.settings.db.db_schema}.blocks_{table}{rl}
            ADD COLUMN IF NOT EXISTS {col} {coltype}
        """
        )

        engine.execute(
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
        engine.dispose()
        return

    @du.recordlinkage_repeat
    def build_forward_indices(self, engine: ENGINE, rl: str = "") -> None:
        """
        Executes SQL queries to build forward indices for train datasets
        """
        engine.execute(
            self.query_blocks(table=f"train{rl}", columns=self.block_scheme_sql)
        )

    @du.recordlinkage_repeat
    def init_forward_index_full(self, engine: ENGINE, rl: str = "") -> None:
        """initialize full index table"""
        engine.execute(
            f"""
            DROP TABLE IF EXISTS {self.settings.db.db_schema}.blocks_df{rl};

            CREATE TABLE {self.settings.db.db_schema}.blocks_df{rl} as (
                SELECT
                    _index
                FROM {self.settings.db.db_schema}.df{rl}
            );
        """
        )

    def build_forward_indices_full(
        self, columns: Tuple[str], engine: ENGINE
    ) -> None:
        """
        Executes SQL queries to build forward indices on full data.

        Parameters
        ----------
        columns : List[str]
            block schemes to include in forward index
        """
        for col in columns:

            logging.info(
                "building forward index on full data for scheme %s", col
            )

            exists = pd.read_sql(
                f"SELECT * FROM {self.settings.db.db_schema}.blocks_df LIMIT 1",
                con=engine,
            ).columns

            self.add_scheme(table="df", col=col, exists=exists, engine=engine)
