"""
simple version fakes for testing
"""

from typing import Dict, Generator, Set

from .concepts import (
    Attribute,
    Clusterer,
    Conjunction,
    ConjunctionFinder,
    Entity,
    Label,
    Pair,
    Record,
    Classifier,
    ActiveLearner,
    Prediction,
)
from .schemes import first_letter_first_word
from pandera.typing import DataFrame
from pandas import concat


class FakeConjunctionFinder(ConjunctionFinder):
    @staticmethod
    def get_best_conjunctions(
        records: DataFrame[Record],
        attributes: Set[Attribute],
        labels: DataFrame[Label],
    ) -> Generator[Conjunction, None, None]:
        for attribute in attributes:
            yield {(first_letter_first_word, attribute)}


class FakeClassifier(Classifier):
    def learn(
        self,
        records: DataFrame[Record],
        attributes: Set[Attribute],
        labels: DataFrame[Label],
    ) -> None:
        pass

    def predict(
        self,
        records: DataFrame[Record],
        attributes: Set[Attribute],
        pairs: DataFrame[Pair],
    ) -> DataFrame[Prediction]:
        predictions = pairs.copy()
        predictions[Prediction.prob] = 0.5
        return predictions


class FakeClusterer(Clusterer):
    @staticmethod
    def get_clusters(
        predictions: DataFrame[Prediction],
    ) -> DataFrame[Entity]:
        return (
            concat(
                [
                    predictions[[field]].rename(columns={field: Entity.id})
                    for field in [Prediction.id1, Prediction.id2]
                ]
            )
            .drop_duplicates()
            .assign(**{Entity.entity_id: lambda _: range(len(_))})
        )


class FakeActiveLearner(ActiveLearner):
    @staticmethod
    def get_next_to_label(
        predictions: DataFrame[Prediction],
    ) -> DataFrame[Pair]:
        return predictions[[Pair.id1, Pair.id2]]
