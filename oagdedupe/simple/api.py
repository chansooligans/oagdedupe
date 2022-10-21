"""
top-level API for this simple dedupe version
"""

from .concepts import Record, Entity, LabelRepository, ConjunctionFinder
from .subroutines import make_initial_labels, get_pairs_one_conjunction
from typing import Set


def dedupe(
    records: FrozenSet[Record],
    label_repo: LabelRepository,
    conj_finder: ConjunctionFinder,
) -> Set[Entity]:
    label_repo.add_all(make_initial_labels(records))
    best_conj = conj_finder.get_best_conjunctions(
        records, label_repo.get()
    )
    pairs = 
