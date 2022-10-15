from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Tuple

from oagdedupe._typing import StatsDict
from oagdedupe.block.schemes import BlockSchemes
from oagdedupe.db.base import BaseComputeBlocking
from oagdedupe.settings import Settings


@dataclass
class BaseOptimizer(ABC, BlockSchemes):
    """Abstract class for all conjunction optimizing algorithms to inherit"""

    compute: BaseComputeBlocking
    settings: Settings

    @abstractmethod
    def get_best(self, scheme: Tuple[str]) -> Optional[List[StatsDict]]:
        return


@dataclass
class BaseForward(ABC, BlockSchemes):
    """Abstract class for building forward index"""

    compute: BaseComputeBlocking
    settings: Settings

    @abstractmethod
    def build_forward_indices(self):
        return


@dataclass
class BaseConjunctions(ABC, BlockSchemes):
    """Abstract class for building conjunction lists"""

    optimizer: BaseOptimizer
    settings: Settings

    @property
    @abstractmethod
    def conjunctions_list(self):
        return


@dataclass
class BasePairs(ABC):
    """Abstract class for applying conjunctions to obtain comparison pairs"""

    compute: BaseComputeBlocking
    settings: Settings

    @abstractmethod
    def add_new_comparisons(self):
        return
