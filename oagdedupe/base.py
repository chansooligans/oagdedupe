from abc import ABC, abstractmethod
from dataclasses import dataclass

from dependency_injector.wiring import Provide

from oagdedupe.block import base as block
from oagdedupe.containers import Container
from oagdedupe.settings import Settings


# blocking
@dataclass
class BaseBlocking(ABC):
    settings: Settings = Provide[Container.settings]

    def __init__(self):
        self.forward = block.BaseForward
        self.conj = block.BaseConjunctions
        self.pairs = block.BasePairs


# cluster
@dataclass
class BaseCluster(ABC):
    """Abstract base class for all clustering algos to inherit"""

    settings: Settings = Provide[Container.settings]
    compute = Provide[Container.compute]

    @abstractmethod
    def get_df_cluster(self):
        return
