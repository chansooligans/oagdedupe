from oagdedupe.distance.string import RayAllJaro
from oagdedupe.cluster.cluster import ConnectedComponents
from oagdedupe.settings import Settings
from oagdedupe.block import Blocker, Conjunctions
from oagdedupe.db.initialize import Initialize
from oagdedupe.db.database import DatabaseORM

import requests
import json
from abc import ABCMeta, abstractmethod
from typing import List, Optional, Tuple
from dataclasses import dataclass
from functools import cached_property
import pandas as pd
import numpy as np
import ray
from sqlalchemy import create_engine
import logging

root = logging.getLogger()
root.setLevel(logging.DEBUG)


class BaseModel(metaclass=ABCMeta):
    """Abstract base class from which all model classes inherit.
    All descendent classes must implement predict, train, and candidates methods.
    """

    """project settings"""

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

    @cached_property
    def engine(self):
        return create_engine(self.settings.other.path_database)

    @cached_property
    def init(self):
        return Initialize(settings=self.settings)

    @cached_property
    def orm(self):
        return DatabaseORM(settings=self.settings)

    @cached_property
    def blocker(self):
        return Blocker(settings=self.settings)

    @cached_property
    def cover(self):
        return Conjunctions(settings=self.settings)
    
    @cached_property
    def distance(self):
        return RayAllJaro(settings=self.settings)

@dataclass
class FitModel:

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

@dataclass
class Dedupe(FitModel, BaseModel):
    """General dedupe block, inherits from BaseModel."""
    settings:Settings

    def __post_init__(self):
        self.settings.sync()
        if (self.settings.other.cpus > 1) & (not ray.is_initialized()):
            ray.init(num_cpus=self.settings.other.cpus)
    
    def predict(self) -> pd.DataFrame:
        """get clusters of matches and return cluster IDs"""

        idxmat, scores, y = self.fit_model()
        self.cluster = ConnectedComponents(settings=self.settings)
        logging.info("get clusters")
        return self.cluster.get_df_cluster(
            matches=idxmat[y == 1].astype(int), scores=scores[y == 1]
        )

    def fit_blocks(self, n_covered=2_000_000):

        # fit block scheme conjunctions to full data
        self.blocker.init_forward_index_full()
        self.cover.save_best(
            table="blocks_df", newtable="full_comparisons", n_covered=n_covered
        )

        # get distances
        self.distance.save_distances(
            table=self.orm.FullComparisons,
            newtable=self.orm.FullDistances
        )

    def initialize(
        self, 
        df=None, 
        reset=True, 
        resample=False, 
        n_covered=500
        ):
        """learn p(match)"""

        self.init.setup(df=df, reset=reset, resample=resample)
        
        self.blocker.build_forward_indices()
        self.cover.save_best(
            table="blocks_train", newtable="comparisons", n_covered=n_covered
        )

        logging.info("get distance matrix")
        self.distance.save_distances(
            table=self.orm.Comparisons,
            newtable=self.orm.Distances
        )

        
@dataclass
class RecordLinkage(FitModel, BaseModel):
    """General dedupe block, inherits from BaseModel."""
    settings:Settings

    def __post_init__(self):
        self.settings.sync()
        if (self.settings.other.cpus > 1) & (not ray.is_initialized()):
            ray.init(num_cpus=self.settings.other.cpus)
    
    def predict(self) -> pd.DataFrame:
        """get clusters of matches and return cluster IDs"""

        idxmat, scores, y = self.fit_model()
        self.cluster = ConnectedComponents(settings=self.settings)
        logging.info("get clusters")
        return self.cluster.get_df_cluster(
            idxmat[y == 1].astype(int), scores[y == 1]
        )

    def fit_blocks(self, n_covered=500_000):

        # fit block scheme conjunctions to full data
        self.blocker.init_forward_index_full()
        self.cover.save_best(
            table="blocks_df", newtable="full_comparisons", n_covered=n_covered
        )

        # get distances
        self.distance.save_distances(
            table=self.orm.FullComparisons,
            newtable=self.orm.FullDistances
        )

    def initialize(
        self, 
        df=None, 
        df2=None,
        reset=True, 
        resample=False, 
        n_covered=500
        ):
        """learn p(match)"""

        self.init.setup(df=df, df2=df2, reset=reset, resample=resample)
        
        self.blocker.build_forward_indices()
        self.cover.save_best(
            table="blocks_train", newtable="comparisons", n_covered=n_covered
        )

        logging.info("get distance matrix")
        self.distance.save_distances(
            table=self.orm.Comparisons,
            newtable=self.orm.Distances
        )

        
        
        


