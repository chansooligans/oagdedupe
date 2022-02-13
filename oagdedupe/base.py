from abc import ABCMeta, abstractmethod
from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass

class BaseBlocker(metaclass=ABCMeta):
    """ Abstract base class for all blockers to inherit
    """

    @abstractmethod
    def ...

class BaseBlockAlgos(metaclass=ABCMeta):
    """ Abstract base class for all blocking algos to inherit
    """

    @abstractmethod
    def ...

class BaseDistance(metaclass=ABCMeta):
    """ Abstract base class for all distance configurations to inherit
    """

    @abstractmethod
    def ...

class BaseTrain(metaclass=ABCMeta):
    """ Abstract base class for all training algos to inherit
    """

    @abstractmethod
    def ...

class BaseCluster(metaclass=ABCMeta):
    """ Abstract base class for all clustering algos to inherit
    """

    @abstractmethod
    def ...