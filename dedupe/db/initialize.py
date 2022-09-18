from dedupe.settings import Settings
from dedupe.distance.string import RayAllJaro
from dedupe.db.engine import Engine

from dataclasses import dataclass
from sqlalchemy import create_engine
from functools import cached_property
import logging

@dataclass
class Initialize(Engine):
    settings:Settings

    def __post_init__(self):
        self.schema = self.settings.other.db_schema
        self.distance = RayAllJaro(settings=self.settings)

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