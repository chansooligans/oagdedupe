from dedupe.base import BaseDistance
from dedupe.db.database import DatabaseORM
from dedupe.settings import Settings

from dataclasses import dataclass
from jellyfish import jaro_winkler_similarity
import ray
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

        if table == "labels":
            comps = self.orm.get_label_attributes()
        else:
            comps = self.orm.get_comparison_attributes(table=table)

        logging.info(f"making {len(comps)} comparions for table: {table}")

        return pd.DataFrame(
            np.column_stack(
                [comps.values] + 
                [
                    self.distance(comps[[f"{attribute}_l",f"{attribute}_r"]].values)
                    for attribute in self.settings.other.attributes
                ]
            ),
            columns = list(comps.columns) + self.settings.other.attributes
        )

    @property
    def dist_tables(self):
        return {
            "labels":self.orm.Labels,
            "distances":self.orm.Distances,
            "full_distances":self.orm.FullDistances
        }

    
    def save_distances(self, table="comparisons", newtable="distances"):

        distances = self.get_distmat(
            table=table
        )

        distances[["_index_l","_index_r"]] = distances[["_index_l","_index_r"]].astype(int)
        
        # reset table
        self.orm.engine.execute(f"""
            TRUNCATE TABLE {self.settings.other.db_schema}.{newtable};
        """)

        # insert
        self.orm.bulk_insert(
            df=distances, to_table=self.dist_tables[newtable]
        )

@ray.remote
def ray_distance(pairs):
    return [
        jaro_winkler_similarity(pair[0], pair[1])
        for pair in pairs
    ]

@dataclass
class RayAllJaro(BaseDistance, DistanceMixin, DatabaseORM):
    settings: Settings
    "needs work: update to allow user to specify attribute-algorithm pairs"

    def __post_init__(self):
        self.orm = DatabaseORM(settings=self.settings)


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
