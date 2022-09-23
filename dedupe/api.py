from dedupe.distance.string import RayAllJaro
from dedupe.cluster.cluster import ConnectedComponents
from dedupe.settings import Settings
from dedupe.block import Blocker, Conjunctions
from dedupe.db.initialize import Initialize
from dedupe.db.database import DatabaseORM

import requests
import json
from abc import ABCMeta, abstractmethod
from typing import List, Optional, Tuple
from dataclasses import dataclass
import pandas as pd
import numpy as np
import ray
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

    @abstractmethod
    def predict(self):
        return

    @abstractmethod
    def fit_blocks(self):
        return

    @abstractmethod
    def fit_model(self):
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
        
        self.init = Initialize(settings=self.settings)
        self.orm = DatabaseORM(settings=self.settings)
        self.blocker = Blocker(settings=self.settings)
        self.cover = Conjunctions(settings=self.settings)
        self.distance = RayAllJaro(settings=self.settings)

    def predict(self) -> pd.DataFrame:
        """get clusters of matches and return cluster IDs"""

        idxmat, scores, y = self.fit_model()

        self.cluster = ConnectedComponents(settings=self.settings)
        
        logging.info("get clusters")
        return self.cluster.get_df_cluster(
            matches=idxmat[y == 1].astype(int), scores=scores[y == 1]
        )


    def fit_model(self) -> Tuple[np.array, np.array, np.array]:
        """learn p(match)"""

        # get predictions
        results = json.loads(
            requests.get(f"{self.settings.other.fast_api.url}/predict").content
        )

        return (
            self.orm.get_full_comparison_indices().values,
            np.array(results["predict_proba"]),
            np.array(results["predict"])
        )

    def fit_blocks(self):

        # fit block scheme conjunctions to full data
        columns = [
            f"{self.blocker.block_scheme_mapping[x]} as {x}"
            for x in set(sum(self.cover.best_schemes(n_covered=5).values, []))
        ]
        self.blocker.build_forward_indices_full(
            columns = columns
        )
        self.cover.save_best(
            table="blocks_df", newtable="full_comparisons", n_covered=5
        )

        # get distances
        self.distance.save_distances(
            table=self.orm.FullComparisons,
            newtable=self.orm.FullDistances
        )

    def initialize(self, df=None, reset=True, resample=False):
        """learn p(match)"""

        self.init.setup(df=df, reset=reset, resample=resample)
        
        self.blocker.build_forward_indices()
        self.cover.save_best(
            table="blocks_train", newtable="comparisons", n_covered=100
        )

        logging.info("get distance matrix")
        self.distance.save_distances(
            table=self.orm.Comparisons,
            newtable=self.orm.Distances
        )

        
        


