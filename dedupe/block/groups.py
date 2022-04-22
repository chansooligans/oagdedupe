"""tools for intersections of method-attribute pairs
"""
from dataclasses import dataclass
from typing import List, FrozenSet
from oagdedupe.base import BaseBlockAlgo

@dataclass(frozen=True)
class Pair:
    """method-attribute pair"""

    BlockAlgo: BaseBlockAlgo
    attribute: str

    def __str__(self) -> str:
        return f"{self.BlockAlgo.__name__}-{self.attribute}"

@dataclass
class Intersection:
    """intersection of Pairs"""
    pairs: List[Pair]

    @property
    def set_pairs(self) -> FrozenSet[Pair]:
        return frozenset(self.pairs)

    def __eq__(self, other) -> bool:
        return self.set_pairs == other.set_pairs

    def __str__(self) -> str:
        return " and ".join(sorted([str(pair) for pair in self.set_pairs]))

@dataclass
class Union:
    """union of intersections"""
    intersections: List[Intersection]

    @property
    def set_pairs(self) -> FrozenSet[FrozenSet[Pair]]:
        return frozenset(inter.set_pairs for inter in self.intersections)

    def __eq__(self, other) -> bool:
        return self.set_pairs == other.set_pairs

    def __str__(self) -> str:
        return " or ".join(
            f"({i})"
            for i in sorted(set([str(inter) for inter in self.intersections]))
        )