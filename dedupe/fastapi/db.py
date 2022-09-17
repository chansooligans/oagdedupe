from dedupe.settings import Settings
import pandas as pd

class Database:
    settings: Settings

    def get_labels(self):
        return pd.read_sql(
            f"SELECT * FROM {self.settings.other.db_schema}.labels",
            con=self.settings.other.engine
        )

    def get_train(self):
        return pd.read_sql(
            f"SELECT * FROM {self.settings.other.db_schema}.train",
            con=self.settings.other.engine
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
            con=self.settings.other.engine
        )

    def get_compare_cols(self):
        columns = [
            [f"{x}_l" for x in self.settings.other.attributes], 
            ["_index_l"],
            [f"{x}_r" for x in self.settings.other.attributes],
            ["_index_r"]
        ]
        return sum(columns, [])