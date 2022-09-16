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

    def get_comparison_attributes(self, table, schema, engine, attributes):
        
        if table == "comparisons":
            return {
                attribute: pd.read_sql(
                    f"""
                        SELECT t2.{attribute}, t3.{attribute}
                        FROM {schema}.comparisons t1 
                        LEFT JOIN {schema}.sample t2 
                        ON t1._index_l = t2._index
                        LEFT JOIN {schema}.sample t3 
                        ON t1._index_r = t3._index
                        ORDER BY t1._index_l, t1._index_r
                    """, 
                    con=engine
                ).values
                for attribute in attributes
            }
        elif table == "labels":
            return {
                attribute: pd.read_sql(
                    f"""
                        WITH 
                            train AS (
                                SELECT distinct on (_index) _index as d, *
                                FROM {schema}.train
                            )
                        SELECT t2.{attribute}, t3.{attribute}
                        FROM {schema}.labels t1 
                        LEFT JOIN train t2 
                        ON t1._index_l = t2._index
                        LEFT JOIN train t3 
                        ON t1._index_r = t3._index
                        ORDER BY t1._index_l, t1._index_r
                    """, 
                    con=engine
                ).values
                for attribute in attributes
            }

    def get_chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def get_distmat(self, table, schema, engine, attributes) -> np.array:
        """for each candidate pair and attribute, compute distances"""

        comparisons = pd.read_sql(
            f"SELECT * FROM {schema}.{table} ORDER BY _index_l, _index_r",
            con=engine
        )
        logging.info(f"making {len(comparisons)} comparions")

        comparison_attributes = self.get_comparison_attributes(table, schema, engine, attributes)

        distances = pd.DataFrame(
            np.column_stack(
                [comparisons.values] + 
                [
                    self.distance(comparison_attributes[attribute])
                    for attribute in attributes
                ]
            ),
            columns = list(comparisons.columns) + attributes
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
