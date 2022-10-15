from abc import ABC, abstractmethod
from dataclasses import dataclass

from oagdedupe.block import base as block
from oagdedupe.block.forward import Forward
from oagdedupe.block.learner import Conjunctions
from oagdedupe.block.pairs import Pairs
from oagdedupe.db.base import BaseCompute
from oagdedupe.settings import Settings


# blocking
@dataclass
class BaseBlocking(ABC):

    compute: BaseCompute
    conj: block.BaseConjunctions = Conjunctions
    forward: block.BaseForward = Forward
    pairs: block.BasePairs = Pairs
    optimizer: block.BaseConjunctions = None


# cluster
@dataclass
class BaseCluster(ABC):
    """Abstract base class for all clustering algos to inherit"""

    compute: BaseCompute
    settings: Settings

    @abstractmethod
    def get_df_cluster(self):
        return
