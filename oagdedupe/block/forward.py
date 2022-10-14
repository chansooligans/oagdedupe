"""This module contains objects used to construct blocks by
creating forward index.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
from dependency_injector.wiring import Provide

from oagdedupe import utils as du
from oagdedupe._typing import ENGINE
from oagdedupe.base import BaseCompute
from oagdedupe.block.base import BaseForward
from oagdedupe.block.schemes import BlockSchemes
from oagdedupe.containers import Container
from oagdedupe.settings import Settings


@dataclass
class Forward(BlockSchemes, BaseForward):
    """
    Used to build forward indices. A forward index
    is a table where rows are entities, columns are block schemes,
    and values contain signatures.

    Attributes
    ----------
    settings : Settings
    """

    settings: Settings = Provide[Container.settings]
    compute: BaseCompute = Provide[Container.blocking]

    @du.recordlinkage_repeat
    def build_forward_indices(self, engine: ENGINE, rl: str = "") -> None:
        """
        Executes SQL queries to build forward indices for train datasets
        """
        self.compute.build_forward_indices(engine=engine)

    def build_forward_indices_full(
        self, columns: Tuple[str], engine: ENGINE, iter: int
    ) -> None:
        """
        Executes SQL queries to build forward indices on full data.

        Parameters
        ----------
        columns : List[str]
            block schemes to include in forward index
        """
        self.compute.build_forward_indices_full(
            columns=columns, engine=engine, iter=iter
        )
