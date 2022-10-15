from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Tuple

from dependency_injector.wiring import Provide

from oagdedupe._typing import StatsDict
from oagdedupe.block.schemes import BlockSchemes
from oagdedupe.containers import Container
from oagdedupe.settings import Settings


@dataclass
class BaseOptimizer(ABC, BlockSchemes):
    """Abstract base class for all conjunction optimizing algorithms to inherit"""

    settings: Settings = Provide[Container.settings]
    compute = Provide[Container.blocking]

    @abstractmethod
    def get_best(self, scheme: Tuple[str]) -> Optional[List[StatsDict]]:
        return


@dataclass
class BaseForward(ABC, BlockSchemes):
    settings: Settings = Provide[Container.settings]
    compute = Provide[Container.blocking]

    @abstractmethod
    def build_forward_indices(self):
        return


@dataclass
class BaseConjunctions(ABC, BlockSchemes):
    optimizer: BaseOptimizer
    settings: Settings = Provide[Container.settings]

    @property
    @abstractmethod
    def conjunctions_list(self):
        return


@dataclass
class BasePairs(ABC):
    settings: Settings = Provide[Container.settings]
    compute = Provide[Container.blocking]

    @abstractmethod
    def add_new_comparisons(self):
        return
