from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from oagdedupe._typing import StatsDict
from oagdedupe.settings import Settings


class BaseOptimizer(ABC):
    """Abstract base class for all conjunction optimizing algorithms to inherit"""

    settings: Settings

    @abstractmethod
    def get_best(self, scheme: Tuple[str]) -> Optional[List[StatsDict]]:
        return


class BaseForward(ABC):
    @abstractmethod
    def build_forward_indices(self):
        return

    @abstractmethod
    def build_forward_indices_full(self):
        return

    @abstractmethod
    def init_forward_index_full(self):
        return


class BaseConjunctions(ABC):
    settings: Settings
    Optimizer: BaseOptimizer

    @property
    @abstractmethod
    def conjunctions_list(self):
        return


class BasePairs(ABC):
    @abstractmethod
    def add_new_comparisons(self):
        return
