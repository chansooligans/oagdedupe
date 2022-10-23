"""simple version utilities
"""

from typing import FrozenSet

from .concepts import Entity, Record, Pair


def get_singletons(
    records: FrozenSet[Record], pairs: FrozenSet[Pair]
) -> FrozenSet[Entity]:
    records_in_pairs = {record for pair in pairs for record in pair}
    return frozenset(
        {
            Entity(records=frozenset({record}))
            for record in records.difference(records_in_pairs)
        }
    )
