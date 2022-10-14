""" This module computes distance calculations between comparison pairs.
"""

from dataclasses import dataclass

from dependency_injector.wiring import Provide
from sqlalchemy import create_engine, func, insert, select
from sqlalchemy.orm import aliased

from oagdedupe import utils as du
from oagdedupe._typing import SESSION, SUBQUERY, TABLE
from oagdedupe.base import BaseDistance
from oagdedupe.containers import Container
from oagdedupe.db.base import BaseCompute
from oagdedupe.settings import Settings


@dataclass
class AllJaro(BaseDistance):
    """
    Interface to compute distance between comparison pairs along
    common attributes.
    """

    def save_distances(self, full: bool, labels: bool) -> None:
        """
        get comparison attributes from table then compute distances
        for each pair

        Parameters
        ----------
        table: TABLE
        """
        self.compute.save_distances(full=full, labels=labels)
