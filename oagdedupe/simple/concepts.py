"""
Base concepts of a simple version
"""

from dataclasses import dataclass
from enum import Enum, auto
from typing import (
    Set,
    Callable,
    Tuple,
    Dict,
    FrozenSet,
    Generator,
    Type,
    Union,
)
from abc import ABC, abstractmethod, abstractstaticmethod
from frozendict import frozendict

Attribute = str


@dataclass(frozen=True)
class Record:

    # frozendict is just an immutable dict
    # so I can put records in a Set, etc.
    # https://marco-sulla.github.io/python-frozendict/
    values: frozendict

    @staticmethod
    def from_dict(values: Dict[Attribute, str]):  # -> Record
        return Record(frozendict(values))


@dataclass(frozen=True)
class Entity:
    records: FrozenSet[Record]


class Signature(ABC):
    @abstractmethod
    def __eq__(self, other) -> bool:
        pass


Scheme = Callable[[str], Union[str, Signature]]

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


Classifier = Callable[[Pair], Label]


class ClassifierRepository(ABC):
    @abstractmethod
    def add(self, classifier: Classifier) -> None:
        pass

    @abstractmethod
    def get(self) -> Classifier:
        pass


class Clusterer(ABC):
    @abstractstaticmethod
    def get_clusters(
        labels: Dict[Pair, Label],
    ) -> Set[Entity]:
        pass
