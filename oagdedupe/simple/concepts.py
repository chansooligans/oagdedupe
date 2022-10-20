"""
Base concepts of a simple version
"""

from dataclasses import dataclass
from typing import Set, Callable, Any, Tuple, List, Dict
from abc import ABC, abstractmethod, abstractclassmethod

Attribute = str

@dataclass
class Record:
    values: Dict[Attribute, str]

@dataclass
class Entity:
    records: List[Record]

class Scheme(ABC):

    @abstractclassmethod
    def get_signature(record: Record, attribute: Attribute):
        pass

    @abstractclassmethod
    def signatures_match(sigs: Tuple) -> bool:
        pass


Conjunction = Set[Tuple[Scheme, Attribute]]


class Blocker(ABC):
    def __init__(self, records: List[Record]):
        self.records = records

    @abstractmethod
    def get_pairs(self, records):
        pass
