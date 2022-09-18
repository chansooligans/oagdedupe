from dedupe.base import BaseDistance
from dedupe.db.database import Database
from dedupe.db.engine import Engine

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

    def get_chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def get_distmat(self, table) -> np.array:
        """for each candidate pair and attribute, compute distances"""

        comps = self.db.get_comparison_attributes(table=table)

        logging.info(f"making {len(comps)} comparions for table: {table}")

        return pd.DataFrame(
            np.column_stack(
                [comps.values] + 
                [
                    self.distance(comps[[f"{attribute}_l",f"{attribute}_r"]].values)
                    for attribute in self.attributes
                ]
            ),
            columns = list(comps.columns) + self.attributes
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


@ray.remote
def ray_distance(pairs):
    return [
        jaro_winkler_similarity(pair[0], pair[1])
        for pair in pairs
    ]

class RayAllJaro(BaseDistance, DistanceMixin, Engine):
    "needs work: update to allow user to specify attribute-algorithm pairs"

    def __init__(self, settings):
        self.settings = settings
        self.schema = settings.other.db_schema
        self.attributes = settings.other.attributes

        self.db = Database(settings=settings)


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
