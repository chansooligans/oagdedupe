from abc import ABC, abstractmethod
from dataclasses import dataclass

from dependency_injector.wiring import Provide

from oagdedupe.block import base as block
from oagdedupe.block.learner import Conjunctions
from oagdedupe.containers import SettingsContainer
from oagdedupe.db.base import BaseCompute
from oagdedupe.settings import Settings


# blocking
@dataclass
class BaseBlocking(ABC):

    compute: BaseCompute
    conj: block.BaseConjunctions = Conjunctions
    forward: block.BaseForward = None
    optimizer: block.BaseConjunctions = None
    pairs: block.BasePairs = None
    settings: Settings = Provide[SettingsContainer.settings]


# cluster
@dataclass
class BaseCluster(ABC):
    """Abstract base class for all clustering algos to inherit"""

    compute: BaseCompute
    settings: Settings = Provide[SettingsContainer.settings]

    @abstractmethod
    def get_df_cluster(self):
        return
