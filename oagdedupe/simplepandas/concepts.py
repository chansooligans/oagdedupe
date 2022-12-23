"""
Base concepts of a simple version
"""

from typing import (
    Set,
    Callable,
    Tuple,
    Dict,
    Generator,
    Type,
    Union,
)
from abc import ABC, abstractmethod, abstractstaticmethod
from pandera import SchemaModel
from pandera.typing import Series, DataFrame

Attribute = str


class Pair(SchemaModel):
    id1: Series[str]
    id2: Series[int]


class Label(Pair):
    label: Series[bool]


class Prediction(Pair):
    prob: Series[float]


class Entity(SchemaModel):
    id: Series[str]
    entity_id: Series[int]


class Signature(ABC):
    @abstractmethod
    def __eq__(self, other) -> bool:
        pass


Scheme = Callable[[str], Union[str, Signature]]
Conjunction = Set[Tuple[Type[Scheme], Attribute]]


class ConjunctionFinder(ABC):
    @abstractstaticmethod
    def get_best_conjunctions(
        records: DataFrame,
        attributes: Set[Attribute],
        labels: DataFrame[Label],
    ) -> Generator[Conjunction, None, None]:
        # what if this is a generator that yeilds the next best conjunction?
        pass


Classifier = Callable[[DataFrame[Pair]], float]


class ClassifierFinder(ABC):
    @abstractstaticmethod
    def learn_classifier(
        records: DataFrame,
        attributes: Set[Attribute],
        labels: DataFrame[Label],
    ) -> Classifier:
        pass


class ActiveLearner(ABC):
    @abstractstaticmethod
    def get_next_to_label(
        predictions: DataFrame[Prediction],
    ) -> DataFrame[Pair]:
        pass


class LabelRepository(ABC):
    @abstractmethod
    def add(self, pair: Pair, label: bool) -> None:
        pass

    @abstractmethod
    def get(self) -> DataFrame[Label]:
        pass

    def add_all(self, labels: Dict[Pair, bool]) -> None:
        for pair, label in labels.items():
            self.add(pair, label)


class Clusterer(ABC):
    @abstractstaticmethod
    def get_clusters(
        predictions: DataFrame[Prediction],
    ) -> DataFrame[Entity]:
        pass
