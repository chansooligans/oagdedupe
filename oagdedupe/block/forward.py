"""This module contains objects used to construct blocks by
creating forward index.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from oagdedupe._typing import ENGINE
from oagdedupe.block.base import BaseForward
from oagdedupe.block.schemes import BlockSchemes
from oagdedupe.db.base import BaseRepositoryBlocking
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
    repository : BaseRepositoryBlocking
    """

    repo: BaseRepositoryBlocking
    settings: Settings

    def build_forward_indices(
        self,
        rl: str = "",
        full: bool = False,
        iter: Optional[int] = None,
        columns: Optional[Tuple[str]] = None,
    ) -> None:
        """
        Build forward indices for train or full datasets
        """
        self.repo.build_forward_indices(
            rl=rl, full=full, iter=iter, columns=columns
        )
