"""This module contains objects used to construct blocks by
creating forward index.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from oagdedupe._typing import ENGINE
from oagdedupe.block.base import BaseForward
from oagdedupe.block.schemes import BlockSchemes
from oagdedupe.db.base import BaseComputeBlocking
from oagdedupe.settings import Settings


@dataclass
class Forward(BaseForward, BlockSchemes):
    """
    Used to build forward indices. A forward index
    is a table where rows are entities, columns are block schemes,
    and values contain signatures.

    Attributes
    ----------
    settings : Settings
    compute : BaseComputeBlocking
    """

    compute: BaseComputeBlocking
    settings: Settings

    def build_forward_indices(
        self,
        engine: ENGINE,
        rl: str = "",
        full: bool = False,
        iter: Optional[int] = None,
        columns: Optional[Tuple[str]] = None,
    ) -> None:
        """
        Build forward indices for train or full datasets
        """
        self.compute.build_forward_indices(
            engine=engine, rl=rl, full=full, iter=iter, columns=columns
        )
