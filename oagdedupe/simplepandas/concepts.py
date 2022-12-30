"""
Base concepts of a simple version
"""

from typing import (
    Set,
    Callable,
    Tuple,
    Generator,
    Union,
    List,
)
from abc import ABC, abstractmethod
from pandera import SchemaModel
from pandera.typing import Series, DataFrame

Attribute = str


class Record(SchemaModel):
    id: Series[int]


class Pair(SchemaModel):
    id1: Series[int]
    id2: Series[int]


class Label(Pair):
    label: Series[bool]


class Prediction(Pair):
    prob: Series[float]


class Entity(Record):
    entity_id: Series[int]


class Scheme(ABC):
    @staticmethod
    @abstractmethod
    def get(value: str) -> Union[str, List[str]]:
        pass

    @classmethod
    def name_field(self, attribute: Attribute) -> str:
        return f"{self.__name__}_{attribute}"

    @classmethod
    def add(
        self, records: DataFrame[Record], attribute: Attribute
    ) -> DataFrame[Record]:
        if self.name_field(attribute) not in records.columns:
            records[self.name_field(attribute)] = records[attribute].map(self.get)


Conjunction = Set[Tuple[Scheme, Attribute]]


class ConjunctionFinder(ABC):
    @staticmethod
    @abstractmethod
    def get_best_conjunctions(
        records: DataFrame[Record],
        attributes: Set[Attribute],
        labels: DataFrame[Label],
    ) -> Generator[Conjunction, None, None]:
        # what if this is a generator that yeilds the next best conjunction?
        pass


class Classifier(ABC):
    @abstractmethod
    def learn(
        self,
        records: DataFrame[Record],
        attributes: Set[Attribute],
        labels: DataFrame[Label],
    ) -> None:
        pass

    @abstractmethod
    def predict(
        self,
        records: DataFrame[Record],
        attributes: Set[Attribute],
        pairs: DataFrame[Pair],
    ) -> DataFrame[Prediction]:
        pass


class ActiveLearner(ABC):
    @staticmethod
    @abstractmethod
    def get_next_to_label(
        predictions: DataFrame[Prediction],
    ) -> DataFrame[Pair]:
        pass


class LabelRepository(ABC):
    @abstractmethod
    def add(self, labels: DataFrame[Label]) -> None:
        pass

    @abstractmethod
    def get(self) -> DataFrame[Label]:
        pass


class Clusterer(ABC):
    @staticmethod
    @abstractmethod
    def get_clusters(
        predictions: DataFrame[Prediction],
    ) -> DataFrame[Entity]:
        pass
