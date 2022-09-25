from oagdedupe.base import BaseDistance
from oagdedupe.db.database import DatabaseORM
from oagdedupe.settings import Settings

from dataclasses import dataclass
from jellyfish import jaro_winkler_similarity
import ray
import numpy as np
import pandas as pd
import logging

@ray.remote
def ray_distance(pairs):
    return [
        jaro_winkler_similarity(pair[0], pair[1])
        for pair in pairs
    ]

@dataclass
class RayAllJaro(BaseDistance, DatabaseORM):
    """
    Interface to compute distance between comparison pairs along 
    common attributes.
    """
    settings: Settings

    def __post_init__(self):
        self.orm = DatabaseORM(settings=self.settings)

    def distance(self, pairs):
        """
        Parameters
        ----------
        pairs: np.array
            Nx2 array of comparison pairs
        """
        chunks = self.get_chunks(pairs, 10000)
        res = ray.get([
            ray_distance.remote(chunk)
            for chunk in chunks
        ])
        return [
            x for sublist in res for x in sublist
        ]

    def get_chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def get_distmat(self, table) -> np.array:
        """
        get comparison attributes from table then compute distances
        for each pair
        
        Parameters
        ----------
        table: sqlalchemy.orm.decl_api.DeclarativeMeta

        Returns
        ----------
        pd.DataFrame
        """

        comps = self.orm.get_comparison_attributes(table)
        
        logging.info(
            f"making {len(comps)} comparions for table: {table.__tablename__}")

        distances = {
            attribute:self.distance(
                comps[[f"{attribute}_l",f"{attribute}_r"]].values)
            for attribute in self.settings.other.attributes
        }

        return pd.concat([
            comps,
            pd.DataFrame(distances)
        ], axis=1)

    def save_distances(self, table, newtable):
        """
        saves distances to newtable
        """

        distances = self.get_distmat(table=table)
        
        # reset table
        self.orm.engine.execute(f"""
            TRUNCATE TABLE {self.settings.other.db_schema}.{newtable.__tablename__};
        """)

        # insert
        self.orm.bulk_insert(
            df=distances, to_table=newtable
        )
