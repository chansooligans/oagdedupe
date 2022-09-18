from dedupe.base import BaseDistance
from dedupe.settings import Settings

from functools import cached_property
from dataclasses import dataclass
from jellyfish import jaro_winkler_similarity
import ray
from sqlalchemy import create_engine
import sqlalchemy
import numpy as np
import pandas as pd
import logging

@dataclass
class DistanceMixin:
    """
    Mixin class for all distance computers
    """

    def sql_columns(self, attributes):
        return f"""
            {", ".join([f"t2.{x} as {x}_l" for x in list(attributes) + ["_index"]])}, 
            {", ".join([f"t3.{x} as {x}_r" for x in list(attributes) + ["_index"]])} 
        """
    
    def get_comparison_attributes(self, table):
            
        if table == "comparisons":
            return pd.read_sql(
                f"""
                    SELECT 
                        {self.sql_columns(attributes=self.attributes)}
                    FROM {self.schema}.comparisons t1 
                    LEFT JOIN {self.schema}.sample t2 
                    ON t1._index_l = t2._index
                    LEFT JOIN {self.schema}.sample t3 
                    ON t1._index_r = t3._index
                    ORDER BY t1._index_l, t1._index_r
                """, 
                con=self.engine
            )
        elif table == "full_comparisons":
             return pd.read_sql(
                f"""
                    SELECT 
                        {self.sql_columns(attributes=self.attributes)}
                    FROM {self.schema}.full_comparisons t1 
                    LEFT JOIN {self.schema}.df t2 
                    ON t1._index_l = t2._index
                    LEFT JOIN {self.schema}.df t3 
                    ON t1._index_r = t3._index
                    ORDER BY t1._index_l, t1._index_r
                """, 
                con=self.engine
            )
        elif table == "labels":
            return pd.read_sql(
                f"""
                    WITH 
                        train AS (
                            SELECT distinct on (_index) _index as d, *
                            FROM {self.schema}.train
                        )
                    SELECT 
                        t1.label, {self.sql_columns(attributes=self.attributes)}
                    FROM {self.schema}.labels t1 
                    LEFT JOIN train t2 
                    ON t1._index_l = t2._index
                    LEFT JOIN train t3 
                    ON t1._index_r = t3._index
                    ORDER BY t1._index_l, t1._index_r
                """, 
                con=self.engine
            )

    def get_chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def get_distmat(self, table) -> np.array:
        """for each candidate pair and attribute, compute distances"""

        comparison_attributes = self.get_comparison_attributes(table=table)

        logging.info(f"making {len(comparison_attributes)} comparions for table: {table}")

        return pd.DataFrame(
            np.column_stack(
                [comparison_attributes.values] + 
                [
                    self.distance(comparison_attributes[[f"{attribute}_l",f"{attribute}_r"]].values)
                    for attribute in self.attributes
                ]
            ),
            columns = list(comparison_attributes.columns) + self.attributes
        )
    
    def save_distances(self, table="comparisons", newtable="distances"):

        distances = self.get_distmat(
            table=table
        )

        distances.to_sql(
            newtable,
            schema=self.schema,
            if_exists="replace", 
            con=self.engine,
            index=False,
            dtype={
                x:sqlalchemy.types.INTEGER()
                for x in ["_index_l","_index_r"]
            }
        )

    @cached_property
    def engine(self):
        return create_engine(self.settings.other.path_database)


@ray.remote
def ray_distance(pairs):
    return [
        jaro_winkler_similarity(pair[0], pair[1])
        for pair in pairs
    ]

class RayAllJaro(BaseDistance, DistanceMixin):
    "needs work: update to allow user to specify attribute-algorithm pairs"

    def __init__(self, settings):
        self.settings = settings
        self.schema = settings.other.db_schema
        self.attributes = settings.other.attributes


    def distance(self, pairs):
        chunks = self.get_chunks(pairs, 10000)
        res = ray.get([
            ray_distance.remote(chunk)
            for chunk in chunks
        ])
        return [
            x for sublist in res for x in sublist
        ]

    def config(self):
        """
        returns dict mapping each column to distance calculation
        """
        return dict

class AllJaro(BaseDistance, DistanceMixin):
    "needs work: update to allow user to specify attribute-algorithm pairs"

    def distance(self, pairs):
        return [
            jaro_winkler_similarity(pair[0], pair[1])
            for pair in pairs
        ]

    def config(self):
        """
        returns dict mapping each column to distance calculation
        """
        return dict
