""" This module computes distance calculations between comparison pairs.
"""

from dataclasses import dataclass

from dependency_injector.wiring import Provide
from sqlalchemy import create_engine, func, insert, select
from sqlalchemy.orm import aliased

from oagdedupe import utils as du
from oagdedupe._typing import SESSION, SUBQUERY, TABLE
from oagdedupe.base import BaseCompute, BaseDistance
from oagdedupe.containers import Container
from oagdedupe.settings import Settings


@dataclass
class AllJaro(BaseDistance):
    """
    Interface to compute distance between comparison pairs along
    common attributes.
    """

    settings: Settings = Provide[Container.settings]
    compute: BaseCompute = Provide[Container.compute]

    def save_distances(self, full: bool, labels: bool) -> None:
        """
        get comparison attributes from table then compute distances
        for each pair

        Parameters
        ----------
        table: TABLE
        """
        self.compute.save_comparison_attributes_dists(full=full, labels=labels)
