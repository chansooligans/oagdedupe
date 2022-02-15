from abc import ABCMeta, abstractmethod
from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass

class BaseBlocker(metaclass=ABCMeta):
    """ Abstract base class for all blockers to inherit
    """

    @abstractmethod
    def get_block_maps(self):
        return

class BaseBlockAlgo(metaclass=ABCMeta):
    """ Abstract base class for all blocking algos to inherit
    """

    @abstractmethod
    def get_block(self):
        return

class BaseDistance(metaclass=ABCMeta):
    """ Abstract base class for all distance configurations to inherit
    """

    @abstractmethod
    def distance(self):
        return

    @abstractmethod
    def config(self):
        return

class BaseTrain(metaclass=ABCMeta):
    """ Abstract base class for all training algos to inherit
    """

    @abstractmethod
    def fit(self):
        return

class BaseCluster(metaclass=ABCMeta):
    """ Abstract base class for all clustering algos to inherit
    """

    @abstractmethod
    def get_df_cluster(self):
        return