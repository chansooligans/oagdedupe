from dedupe.settings import Settings

from functools import cached_property
from sqlalchemy import create_engine
from dataclasses import dataclass
import pandas as pd

@dataclass
class Database:
    settings: Settings

    @cached_property
    def engine(self):
        return create_engine(
            self.settings.other.path_database, echo=True
        )

    def get_labels(self):
        return pd.read_sql(
            f"SELECT * FROM {self.settings.other.db_schema}.labels",
            con=self.engine
        )

    def get_train(self):
        return pd.read_sql(
            f"SELECT * FROM {self.settings.other.db_schema}.train",
            con=self.engine
        )

    def get_distances(self):
        return pd.read_sql_query(
            f"""
            SELECT t1.*
            FROM {self.settings.other.db_schema}.distances t1
            LEFT JOIN {self.settings.other.db_schema}.labels t2
                ON t1._index_l = t2._index_l
                AND t1._index_r = t2._index_r
            WHERE t2._index_l is null
            """, 
            con=self.engine
        )

    def get_full_distances(self):
        return pd.read_sql_query(
            f"""
            SELECT {", ".join(self.settings.other.attributes)}
            FROM {self.settings.other.db_schema}.full_distances
            ORDER BY _index_l, _index_r
            """, 
            con=self.engine
        )

    def get_compare_cols(self):
        columns = [
            [f"{x}_l" for x in self.settings.other.attributes], 
            ["_index_l"],
            [f"{x}_r" for x in self.settings.other.attributes],
            ["_index_r"]
        ]
        return sum(columns, [])