"""
simple version fakes for testing
"""

from typing import Dict, FrozenSet, Generator, Set

from .concepts import (
    Attribute,
    Clusterer,
    Conjunction,
    ConjunctionFinder,
    Entity,
    Label,
    Pair,
    Record,
)
from .schemes import first_letter_first_word


class FakeConjunctionFinder(ConjunctionFinder):
    @staticmethod
    def get_best_conjunctions(
        records: FrozenSet[Record],
        attributes: Set[Attribute],
        labels: Dict[Pair, Label],
    ) -> Generator[Conjunction, None, None]:
        for attribute in attributes:
            yield {(first_letter_first_word, attribute)}


def fake_classifier(pair: Pair) -> Label:
    return Label.NOT_SAME


class FakeClusterer(Clusterer):
    @staticmethod
    def get_clusters(
        labels: Dict[Pair, Label],
    ) -> Set[Entity]:
        return {
            Entity(pair)
            for pair, label in labels.items()
            if label == Label.SAME
        }
