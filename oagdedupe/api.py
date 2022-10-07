import logging
from abc import ABC
from dataclasses import dataclass
from functools import cached_property
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import create_engine

from oagdedupe.base import BaseBlocking, BaseCluster, BaseDistance, BaseORM
from oagdedupe.block.blocking import Blocking
from oagdedupe.block.forward import Forward
from oagdedupe.block.learner import Conjunctions
from oagdedupe.block.optimizers import DynamicProgram
from oagdedupe.block.pairs import Pairs
from oagdedupe.cluster.cluster import ConnectedComponents
from oagdedupe.db.initialize import Initialize
from oagdedupe.db.orm import DatabaseORM
from oagdedupe.distance.string import AllJaro
from oagdedupe.postgres import funcs
from oagdedupe.settings import Settings

root = logging.getLogger()
root.setLevel(logging.DEBUG)


class BaseModel(ABC):
    """Abstract base class from which all model classes inherit.
    All descendent classes must implement predict, train, and candidates methods.
    """

    def __init__(self, settings: Settings, initialize: Initialize):
        self.settings = settings
        self.init = initialize(settings=self.settings)

        self.orm: BaseORM = DatabaseORM(settings=self.settings)

        self.blocking: BaseBlocking = Blocking(
            settings=self.settings,
            forward=Forward(settings=self.settings),
            conj=Conjunctions(
                settings=self.settings,
                optimizer=DynamicProgram(settings=self.settings),
            ),
            pairs=Pairs(settings=self.settings),
        )

        self.distance: BaseDistance = AllJaro(settings=self.settings)

        self.cluster: BaseCluster = ConnectedComponents(settings=self.settings)

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

        logging.info("getting comparisons")
        self.blocking.save(engine=self.engine, full=True)

        # get distances
        logging.info("computing distances")
        self.distance.save_distances(
            table=self.orm.FullComparisons, newtable=self.orm.FullDistances
        )

    @cached_property
    def engine(self) -> sqlalchemy.engine:
        return create_engine(self.settings.other.path_database)


@dataclass
class Dedupe(BaseModel):
    """General dedupe block, inherits from BaseModel."""

    settings: Settings

    def __post_init__(self):
        self.settings.sync()
        funcs.create_functions(settings=self.settings)

    def initialize(
        self,
        df: pd.DataFrame,
        reset: bool = True,
        resample: bool = False,
    ) -> None:
        """learn p(match)"""

        self.init.setup(df=df, df2=None, reset=reset, resample=resample)

        logging.info("computing distances for labels")
        self.init._label_distances()

        logging.info("getting comparisons")
        self.blocking.save(engine=self.engine, full=False)

        logging.info("get distance matrix")
        self.distance.save_distances(
            table=self.orm.Comparisons, newtable=self.orm.Distances
        )


@dataclass
class RecordLinkage(BaseModel):
    """General dedupe block, inherits from BaseModel."""

    settings: Settings

    def __post_init__(self):
        self.settings.sync()
        funcs.create_functions(settings=self.settings)

    def initialize(
        self,
        df: pd.DataFrame,
        df2: pd.DataFrame,
        reset: bool = True,
        resample: bool = False,
    ) -> None:
        """learn p(match)"""

        self.init.setup(df=df, df2=df2, reset=reset, resample=resample)

        logging.info("computing distances for labels")
        self.init._label_distances()

        logging.info("getting comparisons")
        self.blocking.save(engine=self.engine, full=False)

        logging.info("get distance matrix")
        self.distance.save_distances(
            table=self.orm.Comparisons, newtable=self.orm.Distances
        )


@dataclass
class Fapi(BaseModel):
    """General dedupe block, inherits from BaseModel."""

    settings: Settings

    def __post_init__(self):
        self.settings.sync()
        funcs.create_functions(settings=self.settings)

    def initialize(self) -> None:
        """learn p(match)"""

        self.init.setup(reset=False, resample=True)

        logging.info("computing distances for labels")
        self.init._label_distances()

        logging.info("getting comparisons")
        self.blocking.save(engine=self.engine, full=False)

        logging.info("get distance matrix")
        self.distance.save_distances(
            table=self.orm.Comparisons, newtable=self.orm.Distances
        )
