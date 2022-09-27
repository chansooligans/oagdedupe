from oagdedupe.distance.string import AllJaro
from oagdedupe.cluster.cluster import ConnectedComponents
from oagdedupe.settings import Settings
from oagdedupe.block import Blocker, Conjunctions
from oagdedupe.db.initialize import Initialize
from oagdedupe.db.orm import DatabaseORM
from oagdedupe.postgres import funcs

import requests
import json
from abc import ABCMeta, abstractmethod
from typing import List, Optional, Tuple, Dict
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

    def predict(self) -> pd.DataFrame:
        """ fast-api trains model on latest labels then submits scores to 
        postgres
        
        clusterer loads scores and uses comparison indices and 
        predicted probabilities to generate clusters

        Returns
        -------
        df: pd.DataFrame
            if dedupe, returns single df
            
        df,df2: tuple
            if recordlinkage, two dataframes

        """
        logging.info("get clusters")
        requests.post(f"{self.settings.other.fast_api.url}/predict")
        return self.cluster.get_df_cluster()

    def fit_blocks(self):

        # fit block scheme conjunctions to full data
        logging.info("building forward indices")
        self.blocker.init_forward_index_full()

        logging.info("getting comparisons")
        self.cover.save_comparisons(
            table="blocks_df", 
            n_covered=self.settings.other.n_covered
        )

        # get distances
        logging.info("computing distances")
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
        
        logging.info("building forward indices")
        self.blocker.build_forward_indices()

        logging.info("getting comparisons")
        self.cover.save_comparisons(table="blocks_train", n_covered=n_covered)

        logging.info("get distance matrix")
        self.distance.save_distances(
            table=self.orm.Comparisons,
            newtable=self.orm.Distances
        )

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
        return AllJaro(settings=self.settings)

    @cached_property
    def cluster(self):
        return ConnectedComponents(settings=self.settings)


@dataclass
class Dedupe(BaseModel):
    """General dedupe block, inherits from BaseModel."""
    settings:Settings

    def __post_init__(self):
        self.settings.sync()
        funcs.create_functions(settings=self.settings)

        
@dataclass
class RecordLinkage(BaseModel):
    """General dedupe block, inherits from BaseModel."""
    settings:Settings

    def __post_init__(self):
        self.settings.sync()
        funcs.create_functions(settings=self.settings)
