import logging
from abc import ABCMeta
from dataclasses import dataclass
from functools import cached_property
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import create_engine

from oagdedupe.block.forward import Forward
from oagdedupe.block.learner import Conjunctions
from oagdedupe.cluster.cluster import ConnectedComponents
from oagdedupe.db.initialize import Initialize
from oagdedupe.db.orm import DatabaseORM
from oagdedupe.distance.string import AllJaro
from oagdedupe.postgres import funcs
from oagdedupe.settings import Settings

root = logging.getLogger()
root.setLevel(logging.DEBUG)


class BaseModel(metaclass=ABCMeta):
    """Abstract base class from which all model classes inherit.
    All descendent classes must implement predict, train, and candidates methods.
    """

    settings: Settings

    def predict(self) -> Union[pd.DataFrame, Tuple[pd.DataFrame]]:
        """fast-api trains model on latest labels then submits scores to
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

    def fit_blocks(self) -> None:

        # fit block scheme conjunctions to full data
        logging.info("building forward indices")
        self.blocker.init_forward_index_full(engine=self.engine)

        logging.info("getting comparisons")
        self.cover.save_comparisons(
            table="blocks_df",
            n_covered=self.settings.other.n_covered,
            engine=self.engine,
        )

        # get distances
        logging.info("computing distances")
        self.distance.save_distances(
            table=self.orm.FullComparisons, newtable=self.orm.FullDistances
        )

    def initialize(
        self,
        df: Optional[pd.DataFrame] = None,
        df2: Optional[pd.DataFrame] = None,
        reset: bool = True,
        resample: bool = False,
        n_covered: int = 500,
    ) -> None:
        """learn p(match)"""

        self.init.setup(df=df, df2=df2, reset=reset, resample=resample)

        logging.info("computing distances for labels")
        self.init._label_distances()

        logging.info("building forward indices")
        self.blocker.build_forward_indices(engine=self.engine)

        logging.info("getting comparisons")
        self.cover.save_comparisons(
            table="blocks_train", n_covered=n_covered, engine=self.engine
        )

        logging.info("get distance matrix")
        self.distance.save_distances(
            table=self.orm.Comparisons, newtable=self.orm.Distances
        )

    @cached_property
    def engine(self) -> sqlalchemy.engine:
        return create_engine(self.settings.other.path_database)

    @cached_property
    def init(self) -> Initialize:
        return Initialize(settings=self.settings)

    @cached_property
    def orm(self) -> DatabaseORM:
        return DatabaseORM(settings=self.settings)

    @cached_property
    def blocker(self) -> Forward:
        return Forward(settings=self.settings)

    @cached_property
    def cover(self) -> Conjunctions:
        return Conjunctions(settings=self.settings)

    @cached_property
    def distance(self) -> AllJaro:
        return AllJaro(settings=self.settings)

    @cached_property
    def cluster(self) -> ConnectedComponents:
        return ConnectedComponents(settings=self.settings)


@dataclass
class Dedupe(BaseModel):
    """General dedupe block, inherits from BaseModel."""

    settings: Settings

    def __post_init__(self):
        self.settings.sync()
        funcs.create_functions(settings=self.settings)


@dataclass
class RecordLinkage(BaseModel):
    """General dedupe block, inherits from BaseModel."""

    settings: Settings

    def __post_init__(self):
        self.settings.sync()
        funcs.create_functions(settings=self.settings)
