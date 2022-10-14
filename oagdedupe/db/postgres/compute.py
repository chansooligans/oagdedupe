from abc import abstractmethod
from dataclasses import dataclass
from typing import List

import pandas as pd
from dependency_injector.wiring import Provide

from oagdedupe.containers import Container
from oagdedupe.db.base import BaseCompute
from oagdedupe.db.postgres.initialize import Initialize
from oagdedupe.db.postgres.orm import DatabaseORM
from oagdedupe.settings import Settings


@dataclass
class PostgresCompute(BaseCompute):
    """ """

    settings: Settings = Provide[Container.settings]

    def __post_init__(self):
        self.initialize = Initialize(settings=self.settings)
        self.orm = DatabaseORM(settings=self.settings)

    def setup(
        self, df=None, df2=None, reset=True, resample=False, rl: str = ""
    ) -> None:
        """sets up environment

        creates:
        - df
        - pos
        - neg
        - unlabelled
        - train
        - labels
        """
        return self.initialize.setup(
            df=df, df2=df2, reset=reset, resample=resample, rl=rl
        )

    def label_distances(self):
        """computes distances for labels"""
        return self.initialize.label_distances()

    def get_scores(self, threshold) -> pd.DataFrame:
        """returns model predictions"""
        return self.orm.get_scores(threshold=threshold)

    def get_distances(self) -> pd.DataFrame:
        """
        query unlabelled distances for sample data

        Returns
        ----------
        pd.DataFrame
        """
        return self.orm.get_distances()

    def merge_clusters_with_raw_data(self, df_clusters, rl):
        """appends attributes to predictions"""
        return self.orm.merge_clusters_with_raw_data(
            df_clusters=df_clusters, rl=rl
        )

    def save_distances(self, full, labels):
        """computes distances on attributes"""
        return self.orm.save_distances(full=full, labels=labels)

    def get_labels(self):
        return self.orm.get_labels()

    @property
    def compare_cols(self) -> List[str]:
        return self.orm.compare_cols

    def update_train(self, newlabels: pd.DataFrame) -> None:
        """
        for entities that were labelled,
        set "labelled" column in train table to True
        """
        return self.orm.update_train(newlabels=newlabels)

    def update_labels(self, newlabels: pd.DataFrame) -> None:
        """
        add new labels to labels table
        """
        return self.orm.update_labels(newlabels=newlabels)

    def predict(self):
        return self.orm.predict()
