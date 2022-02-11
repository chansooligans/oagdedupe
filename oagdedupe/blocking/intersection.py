"""tools for intersections of method-attribute pairs
"""

from dataclasses import dataclass
from oagdedupe.blocking import blockmethod as bm
from typing import List, FrozenSet


@dataclass
class Intersection:
    pairs: List[bm.Pair]

    @property
    def set_pairs(self) -> FrozenSet[bm.Pair]:
        return frozenset(self.pairs)

    def __eq__(self, other) -> bool:
        return self.set_pairs == other.set_pairs

    def __str__(self) -> str:
        return " and ".join(sorted([str(pair) for pair in self.set_pairs]))
