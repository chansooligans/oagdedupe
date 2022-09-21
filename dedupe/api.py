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

    def fit_blocks(self):

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

    def fit_model(self) -> Tuple[np.array, np.array, np.array]:
        """learn p(match)"""

        # get predictions
        contents = requests.get(f"{self.settings.other.fast_api.url}/predict")
        results = json.loads(contents.content)
        scores = np.array(results["predict_proba"])
        y = np.array(results["predict"])

        idxmat = self.orm.get_full_comparison_indices().values

        return idxmat, scores, y

    def initialize(self, df):
        """learn p(match)"""

        self.init = Initialize(settings=self.settings)

        logging.info(f"building tables in schema: {self.settings.other.db_schema}")
        if df is not None:
            if "_index" in df.columns:
                raise ValueError("_index cannot be a column name")
            self.init._init_df(df=df, attributes=self.settings.other.attributes)
        self.init._init_sample()
        self.init._init_train()
        self.init._init_labels()
        
        self.blocker.build_forward_indices()
        self.cover.save_best()

        logging.info("get distance matrix")
        self.distance.save_distances(
            table="comparisons",
            newtable="distances"
        )

        
        


