from dedupe.base import BaseCluster
from dedupe.distance.string import RayAllJaro
from dedupe.cluster.cluster import ConnectedComponents
from dedupe.settings import Settings
from dedupe.block import Blocker, Coverage

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
    cluster: Optional[BaseCluster] = ConnectedComponents()

    @abstractmethod
    def predict(self):
        return

    @abstractmethod
    def fit(self):
        return

    @abstractmethod
    def initialize(self):
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

        # fit block scheme conjunctions to full data
        columns = [
            f"{self.blocker.block_scheme_mapping[x]} as {x}"
            for x in set(sum(self.cover.best_schemes(n_covered=5).values, []))
        ]
        self.blocker.build_forward_indices_full(
            columns = columns
        )
        self.cover.save_best(table="blocks_df", newtable="full_comparisons", n_covered=5)

        # get distances
        self.distance.save_distances(
            table="full_comparisons",
            newtable="full_distances"
        )

        # get predictions
        contents = requests.get(f"{self.settings.other.fast_api.url}/predict")
        results = json.loads(contents.content)
        scores = np.array(results["predict_proba"])
        y = np.array(results["predict"])

        idxmat = np.array(
            pd.read_sql_query(
                f"""
            SELECT _index_l,_index_r
            FROM {self.schema}.full_distances
            ORDER BY _index_l,_index_r
        """,
                con=self.engine,
            )
        )

        return idxmat, scores, np.array(y)

    def initialize(self, df):
        """learn p(match)"""

        logging.info(f"building tables in schema: {self.settings.other.db_schema}")
        self.blocker = Blocker(settings=self.settings)
        self.blocker.initialize(
            df=df, 
            attributes=self.settings.other.attributes
        )
        self.cover = Coverage(settings=self.settings)
        self.cover.save_best()

        logging.info("get distance matrix")
        self.distance = RayAllJaro(settings=self.settings)
        self.distance.save_distances(
            table="comparisons",
            newtable="distances"
        )

        
        


