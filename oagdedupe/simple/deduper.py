"""
top-level API for this simple dedupe version
"""

from .concepts import (
    Record,
    Entity,
    LabelRepository,
    ClassifierRepository,
    ConjunctionFinder,
    Attribute,
    Clusterer,
    Classifier,
    Pair,
    Label,
)
from .subroutines import get_pairs
from .utils import get_singletons
from typing import FrozenSet, Optional, Set, Dict

from dataclasses import dataclass


@dataclass
class Deduper:
    records: FrozenSet[Record]
    attributes: Set[Attribute]
    conj_finder: ConjunctionFinder
    label_repo: LabelRepository
    classifier_repo: ClassifierRepository
    clusterer: Clusterer
    pair_limit: int = 1000

    def __post_init__(self):
        self.start_fast_api()
        self.start_label_studio()
        self.connect_fast_api_label_studio()

    def start_fast_api(self) -> None:
        pass

    def start_label_studio(self) -> None:
        pass

    def connect_fast_api_label_studio(self) -> None:
        pass

    def get_entities(self) -> Set[Entity]:
        pairs = get_pairs(
            records=self.records,
            conjs=self.conj_finder.get_best_conjunctions(
                records=self.records,
                attributes=self.attributes,
                labels=self.label_repo.get(),
            ),
            limit=self.pair_limit,
        )
        classifier: Optional[Classifier] = self.classifier_repo.get()
        preds: Dict[Pair, Label] = (
            {pair: classifier(pair) for pair in pairs} if classifier else {}
        )
        return self.clusterer.get_clusters(preds).union(
            get_singletons(self.records, frozenset(preds.keys()))
        )
