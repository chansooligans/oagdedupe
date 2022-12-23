"""
top-level API for this simple dedupe version
"""

from .concepts import (
    Entity,
    LabelRepository,
    ConjunctionFinder,
    Attribute,
    Clusterer,
    Classifier,
    Pair,
    Label,
    Conjunction,
    ClassifierFinder,
    ActiveLearner,
)
from .subroutines import get_pairs
from .utils import get_singletons
from typing import Set, Generator

from pandera.typing import DataFrame
from pandera import DataFrameSchema, Column, check_types
from pandas import concat

from dataclasses import dataclass


@dataclass
class Deduper:
    attributes: Set[Attribute]
    records: DataFrame
    label_repo: LabelRepository
    conj_finder: ConjunctionFinder
    classifier_finder: ClassifierFinder
    active_learner: ActiveLearner
    clusterer: Clusterer
    limit_pairs: int = 1000
    limit_conjunctions: int = 3

    def __post_init__(self):
        self.schema.validate(self.records)

    @property
    def schema(self) -> DataFrameSchema:
        return DataFrameSchema(
            {
                "id": Column(str),
                **{attr: Column(str) for attr in self.attributes},
            }
        )

    @property  # type: ignore
    @check_types
    def labels(self) -> DataFrame[Label]:
        return self.label_repo.get()

    @property
    def sample(self) -> DataFrame:
        return self.records.sample(5000)

    @property
    def conjunctions(self) -> Generator[Conjunction, None, None]:
        return self.conj_finder.get_best_conjunctions(
            records=self.sample, attributes=self.attributes, labels=self.labels
        )

    @property
    def classifier(self) -> Classifier:
        return self.classifier_finder.learn_classifier(
            records=self.records, attrbutes=self.attributes, labels=self.labels
        )

    @check_types
    def get_pairs(self, records: DataFrame) -> DataFrame[Pair]:
        return get_pairs(
            records=records,
            conjs=self.conjunctions,
            limit_pairs=self.limit_pairs,
            limit_conjunctions=self.limit_conjunctions,
        )

    @property  # type: ignore
    @check_types
    def next_to_label(self) -> DataFrame[Pair]:
        self.active_learner(
            predictions=self.classifier.predict(
                self.get_pairs(records=self.sample)
            )
        ).get_next()

    @property  # type: ignore
    @check_types
    def entities(self) -> DataFrame[Entity]:
        pairs = self.get_pairs(records=self.records)
        entities = self.clusterer.get_clusters(
            predictions=self.classifier.predict(pairs)
        )
        return concat(
            [
                entities,
                get_singletons(
                    ids=self.ids,
                    pairs=pairs,
                    start=entities[Entity.entity_id].max() + 1,
                ),
            ]
        )


# %%
# there are separate tracks:
# - learn a good conjunction from sample and labels
# - learn a classifier from labels
# - use a good conjuction and classifier to cluster on all data

# Process:
# 1. learn a good classifier from the labels
# 2. take a sample (that includes labels)
# 3. generate the best conjunctions from the sample and labels
# 4. decide from the classifier and good conjunctions what to label next
# 5. label
# 6. repeat from 1, or
# 7. do steps 1 through 3
# 8. get all pairs from the top best conjunctions
# 9. classify all of those pairs
# 10. cluster all of those pairs


# %%
from oagdedupe.simplepandas.learner import Learner

# %%
Learner()
# %%
