"""
simple version subroutines
"""

from .concepts import (
    ConjunctionFinder,
    Pair,
    Record,
    Conjunction,
    Scheme,
    Attribute,
    Label,
    Pair,
    Signature,
)
from typing import FrozenSet, Dict, List, Generator
import random

def get_pairs_one_conjunction(
    records: FrozenSet[Record], conj: Conjunction
) -> FrozenSet[Pair]:
    return frozenset(
        {
            frozenset({rec1, rec2})
            for rec1 in records
            for rec2 in records
            if rec1 != rec2
            and all(
                scheme(rec1.values[attribute]) == scheme(rec2.values[attribute])
                for scheme, attribute in conj
            )
        }
    )


def get_pairs(
    records: FrozenSet[Record],
    conjs: Generator[Conjunction, None, None],
    limit: int,
) -> FrozenSet[Pair]:
    pairs = set()
    for conj in conjs:
        new_pairs = pairs.union(get_pairs_one_conjunction(records, conj))
        if len(new_pairs) > limit:
            break
        else:
            pairs = new_pairs
    return frozenset(pairs)


def make_initial_labels(records: FrozenSet[Record]) -> Dict[Pair, Label]:
    num_records = len(records)
    pos = random.sample(records, min(4, num_records))
    neg = random.sample(records, min(4, num_records))
    pairs_pos = [frozenset({rec, rec}) for rec in pos]
    pairs_neg = [
        frozenset({rec1, rec2}) for rec1 in neg for rec2 in neg if rec1 != rec2
    ]
    labels = {
        **{pair_pos: Label.SAME for pair_pos in pairs_pos},
        **{pair_neg: Label.NOT_SAME for pair_neg in pairs_neg},
    }
    return labels
