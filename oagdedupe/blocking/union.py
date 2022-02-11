"""tools for unions of intersections of method-attribute pairs
"""

from dataclasses import dataclass
from oagdedupe.blocking import intersection as bi, blockmethod as bm
from typing import List, FrozenSet


@dataclass
class Union:
    intersections: List[bi.Intersection]

    @property
    def set_pairs(self) -> FrozenSet[FrozenSet[bm.Pair]]:
        return frozenset(inter.set_pairs for inter in self.intersections)

    def __eq__(self, other) -> bool:
        return self.set_pairs == other.set_pairs

    def __str__(self) -> str:
        return " or ".join(
            f"({i})"
            for i in sorted(set([str(inter) for inter in self.intersections]))
        )