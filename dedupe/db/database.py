from dedupe.settings import Settings
from dedupe.db.engine import Engine

from functools import cached_property
from sqlalchemy import create_engine
from dataclasses import dataclass
import pandas as pd

def check_unnest(name):
    if "ngrams" in name:
        return f"unnest({name})"
    return name

def signatures(names):
    return ", ".join([
        f"{check_unnest(name)} as signature{i}" 
        for i,name in  enumerate(names)
    ])

def lr_columns(attributes):
    return f"""
        {", ".join([f"t2.{x} as {x}_l" for x in list(attributes) + ["_index"]])}, 
        {", ".join([f"t3.{x} as {x}_r" for x in list(attributes) + ["_index"]])} 
    """

@dataclass
class Database(Engine):
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

    def get_labels(self):
        return self.query(
            f"SELECT * FROM {self.schema}.labels"
        )

    def get_train(self):
        return self.query(
            f"SELECT * FROM {self.schema}.train"
        )

    def get_inverted_index(self, names, table):
        return self.query(
            f"""
            SELECT 
                {signatures(names)}, 
                ARRAY_AGG(_index ORDER BY _index asc)
            FROM {self.schema}.{table}
            GROUP BY {", ".join([f"signature{i}" for i in range(len(names))])}
            """
        )

    def get_distances(self):
        return self.query(
            f"""
            SELECT t1.*
            FROM {self.schema}.distances t1
            LEFT JOIN {self.schema}.labels t2
                ON t1._index_l = t2._index_l
                AND t1._index_r = t2._index_r
            WHERE t2._index_l is null
            """
        )

    def get_full_distances(self):
        return self.query(
            f"""
            SELECT {", ".join(self.attributes)}
            FROM {self.schema}.full_distances
            ORDER BY _index_l, _index_r
            """
        )

    def get_full_comparison_indices(self):
        return self.query(
            f"""
            SELECT _index_l,_index_r
            FROM {self.schema}.full_distances
            ORDER BY _index_l,_index_r
            """
        )

    def get_compare_cols(self):
        columns = [
            [f"{x}_l" for x in self.attributes], 
            ["_index_l"],
            [f"{x}_r" for x in self.attributes],
            ["_index_r"]
        ]
        return sum(columns, [])
    
    def get_comparison_attributes(self, table):

        fields_table = {
            "comparisons":"sample",
            "full_comparisons":"df",
            "labels":"train"
        }
        
        fields = f"SELECT * FROM {self.schema}.{fields_table[table]}"
        columns = lr_columns(attributes=self.attributes)
        
        if table == "labels":
            fields = f"""SELECT distinct on (_index) _index as d, *
                    FROM {self.schema}.{fields_table[table]}"""
            columns = f"t1.label, {columns}"

        return self.query(f"""
            WITH 
                fields AS (
                    {fields}
                )
            SELECT 
                {columns}
            FROM {self.schema}.{table} t1 
            LEFT JOIN fields t2 
            ON t1._index_l = t2._index
            LEFT JOIN fields t3 
            ON t1._index_r = t3._index
            ORDER BY t1._index_l, t1._index_r
        """)

    @cached_property
    def blocking_schemes(self):
        """
        get all blocking schemes
        """
        return self.query(
            f"SELECT * FROM {self.schema}.blocks_train LIMIT 1"
        ).columns[1:]

    def get_clusters(self):
        return self.query(f"""
            SELECT * 
            FROM {self.schema}.clusters t1
            JOIN {self.schema}.df t2 
                ON t1._index = t2._index
            ORDER BY cluster
        """)
    