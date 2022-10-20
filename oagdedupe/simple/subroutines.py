"""
simple version subroutines
"""

from .concepts import Record, Conjunction, Scheme, Attribute
from typing import Set, Tuple, Any, List


def get_signature(record: Record, attribute: Attribute, scheme: Scheme):
    return scheme.get_signature(record, attribute)


def signatures_match(
    records: Tuple[Record], attribute: Attribute, scheme: Scheme
) -> bool:
    return scheme.signatures_match(
        get_signature(record, attribute, scheme) for record in records
    )


def get_pairs(records: List[Record], conj: Conjunction) -> List[Tuple[Record]]:
    return {
        (rec1, rec2)
        for rec1 in records
        for rec2 in records
        if rec1 != rec2
        and all(
            signatures_match((rec1, rec2), attribute, scheme)
            for scheme, attribute in conj
        )
    }
