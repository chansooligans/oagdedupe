"""
simple version fakes for testing
"""

from .concepts import ConjunctionFinder, Conjunction, Attribute, Record, Pair, Label
from .schemes import FirstLetterFirstWord
from typing import Set, FrozenSet, Dict, Generator


class FakeConjunctionFinder:
    @staticmethod
    def get_best_conjunctions(
        records: FrozenSet[Record],
        attributes: Set[Attribute],
        labels: Dict[Pair, Label],
    ) -> Generator[Conjunction, None, None]:
        for attribute in attributes:
            yield {(FirstLetterFirstWord, attribute)}
