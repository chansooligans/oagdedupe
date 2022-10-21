"""
top-level API for this simple dedupe version
"""

from .concepts import (
    Record,
    Entity,
    LabelRepository,
    ConjunctionFinder,
    Attribute,
)
from .subroutines import make_initial_labels, get_pairs
from typing import Set


def dedupe(
    records: FrozenSet[Record],
    attributes: Set[Attribute],
    label_repo: LabelRepository,
    conj_finder: ConjunctionFinder,
) -> Set[Entity]:
    label_repo.add_all(make_initial_labels(records))
    pairs = get_pairs(
        records,
        conj_finder.get_best_conjunctions(
            records, attributes, label_repo.get()
        ),
        limit=100
    )
