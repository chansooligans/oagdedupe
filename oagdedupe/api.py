import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import requests
import sqlalchemy
from dependency_injector import providers
from sqlalchemy import create_engine

from oagdedupe.base import BaseBlocking, BaseCluster
from oagdedupe.block.blocking import Blocking
from oagdedupe.block.forward import Forward
from oagdedupe.block.learner import Conjunctions
from oagdedupe.block.optimizers import DynamicProgram
from oagdedupe.block.pairs import Pairs
from oagdedupe.cluster.cluster import ConnectedComponents
from oagdedupe.containers import Container
from oagdedupe.db.base import BaseCompute, BaseComputeBlocking
from oagdedupe.db.postgres.blocking import PostgresBlocking
from oagdedupe.db.postgres.compute import PostgresCompute
from oagdedupe.settings import Settings

root = logging.getLogger()
root.setLevel(logging.DEBUG)


@dataclass
class BaseModel(ABC):
    """Abstract base class from which all model classes inherit.
    All descendent classes must implement predict, train, and candidates methods.
    """

    settings: Settings
    compute: BaseCompute = PostgresCompute
    compute_blocking: BaseComputeBlocking = PostgresBlocking
    blocking: BaseBlocking = Blocking
    cluster: BaseCluster = ConnectedComponents

    def __post_init__(
        self,
    ):

        container = Container()

        if self.settings:
            container.settings.override(self.settings)
        if self.compute:
            container.compute.override(providers.Factory(self.compute))
            container.blocking.override(
                providers.Factory(self.compute_blocking)
            )

        self.compute = self.compute(settings=self.settings)

        container.wire(
            packages=[
                "oagdedupe.db",
                "oagdedupe.db.postgres",
                "oagdedupe.block",
                "oagdedupe.cluster",
            ],
        )

        self.blocking = self.blocking(
            forward=Forward(),
            conj=Conjunctions(optimizer=DynamicProgram()),
            pairs=Pairs(),
        )

        self.cluster = self.cluster()

    @abstractmethod
    def initialize(self):
        return

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
        requests.post(f"{self.settings.fast_api.url}/train")
        self.compute.predict()
        return self.cluster.get_df_cluster()

    def fit_blocks(self) -> None:

        logging.info("getting comparisons")
        self.blocking.save(engine=self.engine, full=True)

        # get distances
        logging.info("computing distances")
        self.compute.save_distances(full=True, labels=False)

    @cached_property
    def engine(self) -> sqlalchemy.engine:
        return create_engine(self.settings.db.path_database)


@dataclass
class Dedupe(BaseModel):
    """General dedupe block, inherits from BaseModel."""

    def __post_init__(self):
        super().__post_init__()

    def initialize(
        self,
        df: pd.DataFrame,
        reset: bool = True,
        resample: bool = False,
    ) -> None:
        """learn p(match)"""

        self.compute.setup(df=df, df2=None, reset=reset, resample=resample)

        logging.info("getting comparisons")
        self.blocking.save(engine=self.engine, full=False)

        logging.info("get distance matrix")
        self.compute.save_distances(full=False, labels=False)


@dataclass
class RecordLinkage(BaseModel):
    """General dedupe block, inherits from BaseModel."""

    def __post_init__(self):
        super().__post_init__()

    def initialize(
        self,
        df: pd.DataFrame,
        df2: pd.DataFrame,
        reset: bool = True,
        resample: bool = False,
    ) -> None:
        """learn p(match)"""

        self.compute.setup(df=df, df2=df2, reset=reset, resample=resample)

        logging.info("getting comparisons")
        self.blocking.save(engine=self.engine, full=False)

        logging.info("get distance matrix")
        self.compute.save_distances(full=False, labels=False)


@dataclass
class Fapi(BaseModel):
    """General dedupe block, inherits from BaseModel."""

    def __post_init__(self):
        super().__post_init__()

    def initialize(self) -> None:
        """learn p(match)"""

        self.compute.setup(reset=False, resample=True)

        logging.info("getting comparisons")
        self.blocking.save(engine=self.engine, full=False)

        logging.info("get distance matrix")
        self.compute.save_distances(full=False, labels=False)
