"""This module contains objects used to construct blocks by
creating forward index.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

from oagdedupe import utils as du
from oagdedupe._typing import ENGINE
from oagdedupe.settings import Settings


class BlockSchemes:
    """
    Attributes used to help build SQL queries, which are used to
    build forward indices.
    """

    settings: Settings

    @property
    def block_scheme_mapping(self) -> Dict[str, str]:
        """
        helper to build column names in query
        """
        mapping = {}
        for attribute in self.settings.other.attributes:
            for scheme, nlist in self.block_schemes:
                for n in nlist:
                    if n:
                        mapping[
                            f"{scheme}_{n}_{attribute}"
                        ] = f"{scheme}({attribute},{n})"
                    else:
                        mapping[
                            f"{scheme}_{attribute}"
                        ] = f"{scheme}({attribute})"
        return mapping

    @property
    def block_scheme_sql(self) -> List[str]:
        """
        helper to build column names in query
        """
        return [
            f"{scheme}({attribute},{n}) as {scheme}_{n}_{attribute}"
            if n
            else f"{scheme}({attribute}) as {scheme}_{attribute}"
            for attribute in self.settings.other.attributes
            for scheme, nlist in self.block_schemes
            for n in nlist
        ]

    @property
    def block_schemes(self) -> List[Tuple[str, List[Optional[int]]]]:
        """
        List of tuples containing block scheme name and parameters.
        The block scheme name should correspond to a postgres function
        """
        return [
            ("first_nchars", [2, 4, 6]),
            ("last_nchars", [2, 4, 6]),
            ("find_ngrams", [4, 6, 8]),
            ("acronym", [None]),
            ("exactmatch", [None]),
        ]

    @property
    def block_scheme_names(self) -> List[str]:
        """
        Convenience property to list all block schemes
        """
        return [
            f"{scheme}_{n}_{attribute}" if n else f"{scheme}_{attribute}"
            for attribute in self.settings.other.attributes
            for scheme, nlist in self.block_schemes
            for n in nlist
        ]


@dataclass
class Forward(BlockSchemes):
    """
    Used to build forward indices. A forward index
    is a table where rows are entities, columns are block schemes,
    and values contain signatures.

    Attributes
    ----------
    settings : Settings
    """

    settings: Settings

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
            DROP TABLE IF EXISTS {self.settings.other.db_schema}.blocks_{table};

            CREATE TABLE {self.settings.other.db_schema}.blocks_{table} as (
                SELECT
                    _index,
                    {", ".join(columns)}
                FROM {self.settings.other.db_schema}.{table}
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
            ALTER TABLE {self.settings.other.db_schema}.blocks_{table}{rl}
            ADD COLUMN IF NOT EXISTS {col} {coltype}
        """
        )

        engine.execute(
            f"""
            UPDATE {self.settings.other.db_schema}.blocks_{table}{rl} AS t1
            SET {col} = t2.{col}
            FROM (
                SELECT _index, {self.block_scheme_mapping[col]} as {col}
                FROM {self.settings.other.db_schema}.{table}{rl}
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
            DROP TABLE IF EXISTS {self.settings.other.db_schema}.blocks_df{rl};

            CREATE TABLE {self.settings.other.db_schema}.blocks_df{rl} as (
                SELECT
                    _index
                FROM {self.settings.other.db_schema}.df{rl}
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
                f"SELECT * FROM {self.settings.other.db_schema}.blocks_df LIMIT 1",
                con=engine,
            ).columns

            self.add_scheme(table="df", col=col, exists=exists, engine=engine)
