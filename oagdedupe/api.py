import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import requests

from oagdedupe import db
from oagdedupe.base import BaseCluster
from oagdedupe.block.blocking import Blocking
from oagdedupe.block.optimizers import DynamicProgram
from oagdedupe.cluster.cluster import ConnectedComponents
from oagdedupe.settings import Settings

root = logging.getLogger()
root.setLevel(logging.DEBUG)


@dataclass
class BaseModel(ABC):
    """Abstract base class from which all model classes inherit.
    All descendent classes must implement predict, train, and candidates methods.
    """

    settings: Settings
    cluster: BaseCluster = ConnectedComponents

    def __post_init__(
        self,
    ):
        self.repo = db.get_repository(settings=self.settings)

        self.blocking = Blocking(
            repo=self.repo.blocking,
            optimizer=DynamicProgram,
        )

        self.cluster = self.cluster(settings=self.settings, repo=self.repo)

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
        self.repo.save_predictions()
        return self.cluster.get_df_cluster()

    def fit_blocks(self) -> None:

        logging.info("getting comparisons")
        self.blocking.save(full=True)

        # get distances
        logging.info("computing distances")
        self.repo.save_distances(full=True, labels=False)


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

        self.repo.setup(df=df, df2=None, reset=reset, resample=resample)

        logging.info("getting comparisons")
        self.blocking.save(full=False)

        logging.info("get distance matrix")
        self.repo.save_distances(full=False, labels=True)


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

        self.repo.setup(df=df, df2=df2, reset=reset, resample=resample)

        logging.info("getting comparisons")
        self.blocking.save(full=False)

        logging.info("get distance matrix")
        self.repo.save_distances(full=False, labels=True)


@dataclass
class Fapi(BaseModel):
    """General dedupe block, inherits from BaseModel."""

    def __post_init__(self):
        super().__post_init__()

    def initialize(self) -> None:
        """learn p(match)"""

        self.repo.setup(reset=False, resample=True)

        logging.info("getting comparisons")
        self.blocking.save(full=False)

        logging.info("get distance matrix")
        self.repo.save_distances(full=False, labels=True)
