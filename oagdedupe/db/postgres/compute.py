from abc import abstractmethod
from dataclasses import dataclass

from dependency_injector.wiring import Provide

from oagdedupe.base import BaseCompute
from oagdedupe.containers import Container
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

    def get_scores(self, threshold):
        """returns model predictions"""
        return self.orm.get_scores(threshold=threshold)

    def merge_clusters_with_raw_data(self, df_clusters, rl):
        """appends attributes to predictions"""
        return self.orm.merge_clusters_with_raw_data(
            df_clusters=df_clusters, rl=rl
        )

    def save_comparison_attributes_dists(self, full, labels):
        """computes distances on attributes"""
        return self.orm.save_comparison_attributes_dists(
            full=full, labels=labels
        )
