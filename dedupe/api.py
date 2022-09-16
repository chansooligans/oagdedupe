from dedupe.base import BaseDistance, BaseCluster
from dedupe.distance.string import RayAllJaro
from dedupe.cluster.cluster import ConnectedComponents
from dedupe.settings import Settings

import requests
import json
from abc import ABCMeta, abstractmethod
from typing import List, Optional, Tuple
from dataclasses import dataclass
import pandas as pd
import numpy as np
import ray
import gc
import sqlalchemy
from sqlalchemy import create_engine
import logging

root = logging.getLogger()
root.setLevel(logging.DEBUG)


@dataclass
class BaseModel(metaclass=ABCMeta):
    """Abstract base class from which all model classes inherit.
    All descendent classes must implement predict, train, and candidates methods.
    """

    """project settings"""
    settings: Settings
    df: Optional[pd.DataFrame] = None
    distance: Optional[BaseDistance] = RayAllJaro()
    cluster: Optional[BaseCluster] = ConnectedComponents()

    @abstractmethod
    def predict(self):
        return

    @abstractmethod
    def fit(self):
        return

    @abstractmethod
    def train(self):
        return


@dataclass
class Dedupe(BaseModel):
    """General dedupe block, inherits from BaseModel."""

    def __post_init__(self):

        self.settings.sync()

        self.engine = create_engine(self.settings.other.path_database)

        if (self.settings.other.cpus > 1) & (not ray.is_initialized()):
            ray.init(num_cpus=self.settings.other.cpus)

    def predict(self) -> pd.DataFrame:
        """get clusters of matches and return cluster IDs"""

        idxmat, scores, y = self.fit()

        logging.info("get clusters")
        return self.cluster.get_df_cluster(
            matches=idxmat[y == 1].astype(int), scores=scores[y == 1], rl=False
        )

    def fit(self) -> Tuple[np.array, np.array, np.array]:
        """learn p(match)"""

        contents = requests.get(f"{self.settings.other.fast_api.url}/predict")
        results = json.loads(contents.content)
        scores = np.array(results["predict_proba"])
        y = np.array(results["predict"])

        idxmat = np.array(
            pd.read_sql_query(
                f"""
            SELECT idxl, idxr
            FROM idxmat
        """,
                con=self.engine,
            )
        )

        return idxmat, scores, np.array(y)

    def train(self) -> Tuple[np.array, np.array, np.array]:
        """learn p(match)"""

        logging.info("get distance matrix")
        distances = self.distance.get_distmat(
            table="comparisons",
            schema=self.settings.other.db_schema,
            engine=self.engine,
            attributes=self.settings.other.attributes,
        )

        distances.to_sql(
            "distances",
            schema=self.settings.other.db_schema,
            if_exists="replace", 
            con=self.engine,
            index=False,
            dtype={
                x:sqlalchemy.types.INTEGER()
                for x in ["_index_l","_index_r"]
            }
        )

        


