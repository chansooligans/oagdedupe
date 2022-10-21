"""
simple version subroutines
"""

from .concepts import Pair, Record, Conjunction, Scheme, Attribute
from typing import FrozenSet


def get_signature(record: Record, attribute: Attribute, scheme: Scheme):
    return scheme.get_signature(record, attribute)


def signatures_match(pair: Pair, attribute: Attribute, scheme: Scheme) -> bool:
    return scheme.signatures_match(
        get_signature(record, attribute, scheme) for record in pair
    )


def get_pairs(records: FrozenSet[Record], conj: Conjunction) -> FrozenSet[Pair]:
    return frozenset(
        {
            frozenset({rec1, rec2})
            for rec1 in records
            for rec2 in records
            if rec1 != rec2
            and all(
                signatures_match(frozenset({rec1, rec2}), attribute, scheme)
                for scheme, attribute in conj
            )
        }
    )
