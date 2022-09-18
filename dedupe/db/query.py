from dedupe.settings import Settings

from functools import cached_property
from sqlalchemy import create_engine
from dataclasses import dataclass
import pandas as pd

@dataclass
class Database:
    settings: Settings

    def __post_init__(self):
        self.engine_url = self.settings.other.path_database
        self.schema = self.settings.other.db_schema
        self.attributes = self.settings.other.attributes

    def query(self, sql):
        """
        for parallel implementation, need to create separate engine 
        for each process
        """
        engine = create_engine(self.engine_url)
        res = pd.read_sql(sql, con=engine)
        engine.dispose()
        return res

    @cached_property
    def engine(self):
        return create_engine(
            self.engine_url, echo=True
        )

    def get_labels(self):
        return pd.read_sql(
            f"SELECT * FROM {self.schema}.labels",
            con=self.engine
        )

    def get_train(self):
        return pd.read_sql(
            f"SELECT * FROM {self.schema}.train",
            con=self.engine
        )

    def get_distances(self):
        return pd.read_sql_query(
            f"""
            SELECT t1.*
            FROM {self.schema}.distances t1
            LEFT JOIN {self.schema}.labels t2
                ON t1._index_l = t2._index_l
                AND t1._index_r = t2._index_r
            WHERE t2._index_l is null
            """, 
            con=self.engine
        )

    def get_full_distances(self):
        return pd.read_sql_query(
            f"""
            SELECT {", ".join(self.attributes)}
            FROM {self.schema}.full_distances
            ORDER BY _index_l, _index_r
            """, 
            con=self.engine
        )

    def get_compare_cols(self):
        columns = [
            [f"{x}_l" for x in self.attributes], 
            ["_index_l"],
            [f"{x}_r" for x in self.attributes],
            ["_index_r"]
        ]
        return sum(columns, [])

    @cached_property
    def blocking_schemes(self):
        """
        get all blocking schemes
        """
        return self.query(
            f"SELECT * FROM {self.schema}.blocks_train LIMIT 1"
        ).columns[1:]

    @cached_property
    def n(self):
        """
        sample_n used for reduction ratio computation
        """
        return len(self.query(
            f"SELECT * FROM {self.schema}.sample"
        ))

    @cached_property
    def tables(self):
        return {
            "blocks_train":self.query(
                f"SELECT * FROM {self.schema}.blocks_train"
            ),
            "blocks_sample":self.query(
                f"SELECT * FROM {self.schema}.blocks_sample"
            )
        }

    @cached_property
    def labels(self):
        return self.query(
            f"SELECT * FROM {self.schema}.labels"
        )
