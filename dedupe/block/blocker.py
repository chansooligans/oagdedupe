from dedupe.base import BaseDistance
from dedupe.distance.string import RayAllJaro
from dedupe.settings import Settings
from typing import List, Optional, Tuple

from dataclasses import dataclass
from typing import List
from sqlalchemy import create_engine
from functools import cached_property
import logging
import sqlalchemy

class ForwardIndex:
    """
    Builds Entity Index (Forward Index) where keys are entities and values are signatures
    """

    @property
    def block_schemes(self):
        return [
            ("first_nchars", [2,4,6]),
            ("last_nchars", [2,4,6]),
            ("find_ngrams",[2,4,6]),
            ("acronym", [None]),
            ("exactmatch", [None])
        ]

    @cached_property
    def block_scheme_mapping(self):
        """
        helper to build column names in query
        """
        mapping = {}
        for attribute in self.settings.other.attributes:
            for scheme,nlist in self.block_schemes:
                for n in nlist:
                    if n:
                        mapping[f"{scheme}_{n}_{attribute}"]=f"{scheme}({attribute},{n})"
                    else:
                        mapping[f"{scheme}_{attribute}"]=f"{scheme}({attribute})"
        return mapping

    @cached_property
    def block_scheme_sql(self):
        """
        helper to build column names in query
        """
        return [
            f"{scheme}({attribute},{n}) as {scheme}_{n}_{attribute}"
            if n
            else f"{scheme}({attribute}) as {scheme}_{attribute}"
            for attribute in self.settings.other.attributes
            for scheme,nlist in self.block_schemes
            for n in nlist
        ]

    def query_blocks(self, table, columns):
        return f"""
            DROP TABLE IF EXISTS {self.schema}.blocks_{table};
            
            CREATE TABLE {self.schema}.blocks_{table} as (
                SELECT 
                    _index,
                    {", ".join(columns)}
                FROM {self.schema}.{table}
            );
        """

    def build_forward_indices(self):
        for table in ["sample","train"]:
            logging.info(f"Building forward indices: {self.schema}.blocks_{table}")
            self.engine.execute(self.query_blocks(
                table=table,
                columns=self.block_scheme_sql
            ))

    def build_forward_indices_full(self, columns):
        logging.info(f"Building forward indices: {self.schema}.blocks_df")
        self.engine.execute(self.query_blocks(
            table="df",
            columns=columns
        ))

class Initialize(ForwardIndex):

    def _init_df(self, df, attributes):
        logging.info(f"Building table {self.schema}.df...")
        self.engine.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        df[attributes].to_sql(
            "df", 
            schema=self.schema, 
            con=self.engine, 
            if_exists="replace", 
            index=False
        )
        self.engine.execute(f"ALTER TABLE {self.schema}.df ADD _index SERIAL PRIMARY KEY")
        
    def _init_sample(self):
        logging.info(f"Building table {self.schema}.sample...")
        self.engine.execute(f"""
            DROP TABLE IF EXISTS {self.schema}.sample;
            CREATE TABLE {self.schema}.sample AS (
                SELECT * FROM {self.schema}.df
                ORDER BY random() 
                LIMIT {self.settings.other.n}
            )
        """)

    def _init_train(self):
        logging.info(f"Building table {self.schema}.train...")
        self.engine.execute(f"""
            DROP TABLE IF EXISTS {self.schema}.pos;
            CREATE TABLE {self.schema}.pos AS (
                SELECT *
                FROM {self.schema}.df
                ORDER BY random()
                LIMIT 1
            );
            DROP TABLE IF EXISTS {self.schema}.neg;
            CREATE TABLE {self.schema}.neg AS (
                SELECT *
                FROM {self.schema}.df
                WHERE _index NOT IN (
                    SELECT _index from {self.schema}.pos
                )
                ORDER BY random()
                LIMIT 8
            );
            DROP TABLE IF EXISTS {self.schema}.train;
            CREATE TABLE {self.schema}.train AS (
                SELECT {self.schema}.pos.*
                FROM {self.schema}.pos 
                CROSS JOIN generate_series(1,4) as x
                UNION ALL
                SELECT *
                FROM {self.schema}.neg
            );
        """)

    def _init_labels(self):
        logging.info(f"Building table {self.schema}.labels...")
        self.engine.execute(f"""
            DROP TABLE IF EXISTS {self.schema}.labels;
            CREATE TABLE {self.schema}.labels AS (
                WITH 
                    positive_labels AS (
                        SELECT _index as _index_l, _index  as _index_r, 1 as label
                        FROM {self.schema}.pos
                        CROSS JOIN generate_series(1,10) as x
                    ),
                    negative_labels AS (
                        SELECT t1._index as _index_l, t2._index as _index_r, 0 as label
                        FROM {self.schema}.neg AS t1
                        CROSS JOIN {self.schema}.neg AS t2 
                        WHERE t1._index < t2._index
                    )
                SELECT * FROM positive_labels    
                UNION ALL
                SELECT * FROM negative_labels
            )
        """)

        self.distance.save_distances(
            table="labels",
            newtable="labels"
        )
        
@dataclass
class Blocker(Initialize):
    settings: Settings

    def __post_init__(self):
        self.schema = self.settings.other.db_schema
        self.distance = RayAllJaro(settings=self.settings)

    def initialize(self, df, attributes):
        
        if df is not None:
            if "_index" in df.columns:
                raise ValueError("_index cannot be a column name")
            self._init_df(df=df, attributes=attributes)

        # build SQL tables
        self._init_sample()
        self._init_train()
        self._init_labels()
        self.build_forward_indices()

    @cached_property
    def engine(self):
        return create_engine(self.settings.other.path_database)