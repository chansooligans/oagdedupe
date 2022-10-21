"""
Base concepts of a simple version
"""

from dataclasses import dataclass
from enum import Enum, auto
from typing import (
    Set,
    Callable,
    Any,
    Tuple,
    List,
    Dict,
    FrozenSet,
    Generator,
    Type,
)
from abc import ABC, abstractmethod, abstractclassmethod, abstractstaticmethod
from frozendict import frozendict

Attribute = str


@dataclass(frozen=True)
class Record:

    # frozendict is just an immutable dict
    # so I can put records in Sets, etc.
    # https://marco-sulla.github.io/python-frozendict/
    values: frozendict

    @staticmethod
    def from_dict(values: Dict[Attribute, str]):  # -> Record
        return Record(frozendict(values))


@dataclass(frozen=True)
class Entity:
    records: FrozenSet[Record]


class Scheme(ABC):
    @abstractstaticmethod
    def get_signature(record: Record, attribute: Attribute):
        pass

    @abstractstaticmethod
    def signatures_match(sigs: Tuple) -> bool:
        pass


Pair = FrozenSet[Record]
Conjunction = Set[Tuple[Type[Scheme], Attribute]]


class Label(Enum):
    SAME = auto()
    NOT_SAME = auto()


class ConjunctionFinder(ABC):
    @abstractstaticmethod
    def get_best_conjunctions(
        records: FrozenSet[Record],
        attributes: Set[Attribute],
        labels: Dict[Pair, Label],
    ) -> Generator[Conjunction, None, None]:
        # what if this is a generator that yeilds the next best conjunction?
        pass


class LabelManager(ABC):
    @abstractstaticmethod
    def make_initial_labels(records: FrozenSet[Record]) -> Dict[Pair, Label]:
        pass


class LabelRepository(ABC):
    @abstractmethod
    def add(self, pair: Pair, label: Label) -> None:
        pass

    @abstractmethod
    def get(self) -> Dict[Pair, Label]:
        pass

    def add_all(self, labels: Dict[Pair, Label]) -> None:
        for pair, label in labels.items():
            self.add(pair, label)
