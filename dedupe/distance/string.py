from dedupe.base import BaseDistance
from dataclasses import dataclass
from jellyfish import jaro_winkler_similarity
import ray
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
    
    def get_comparison_attributes(self, table, schema, attributes, engine):
            
        if table == "comparisons":
            return pd.read_sql(
                f"""
                    SELECT 
                        {self.sql_columns(attributes=attributes)}
                    FROM {schema}.comparisons t1 
                    LEFT JOIN {schema}.sample t2 
                    ON t1._index_l = t2._index
                    LEFT JOIN {schema}.sample t3 
                    ON t1._index_r = t3._index
                    ORDER BY t1._index_l, t1._index_r
                """, 
                con=engine
            )
             
        elif table == "labels":
            return pd.read_sql(
                f"""
                    WITH 
                        train AS (
                            SELECT distinct on (_index) _index as d, *
                            FROM {schema}.train
                        )
                    SELECT 
                        t1.label, {self.sql_columns(attributes=attributes)}
                    FROM {schema}.labels t1 
                    LEFT JOIN train t2 
                    ON t1._index_l = t2._index
                    LEFT JOIN train t3 
                    ON t1._index_r = t3._index
                    ORDER BY t1._index_l, t1._index_r
                """, 
                con=engine
            )

    def get_chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def get_distmat(self, table, settings, engine) -> np.array:
        """for each candidate pair and attribute, compute distances"""


        comparison_attributes = self.get_comparison_attributes(
            table=table, 
            schema=settings.other.db_schema, 
            attributes=settings.other.attributes, 
            engine=engine, 
        )

        logging.info(f"making {len(comparison_attributes)} comparions")

        distances = pd.DataFrame(
            np.column_stack(
                [comparison_attributes.values] + 
                [
                    self.distance(comparison_attributes[[f"{attribute}_l",f"{attribute}_r"]].values)
                    for attribute in settings.other.attributes
                ]
            ),
            columns = list(comparison_attributes.columns) + settings.other.attributes
        )
        
        return distances

@ray.remote
def ray_distance(pairs):
    return [
        jaro_winkler_similarity(pair[0], pair[1])
        for pair in pairs
    ]

class RayAllJaro(BaseDistance, DistanceMixin):
    "needs work: update to allow user to specify attribute-algorithm pairs"

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
