"""tools for intersections of method-attribute pairs
"""

from dataclasses import dataclass
from oagdedupe.blocking import blockmethod as bm
from oagdedupe.blocking import intersection as bi, blockmethod as bm
from typing import List, FrozenSet

def get_method(name: str) -> Method:
    """get a method from the name of a method, raises exception if there are no or more than one methods with the given name

    Parameters
    ----------
    name : str
        name of method

    Returns
    -------
    Method
        method matching on name
    """
    matches = {i for i in methods if i.__name__ == name}
    assert (
        len(matches) == 1
    ), f"one match for name {name} not found: matches = {matches}"
    return matches.pop()

@dataclass(frozen=True)
class Pair:
    """method-attribute pair"""

    method: Method
    attribute: str

    def __str__(self) -> str:
        return f"{self.method.__name__}-{self.attribute}"


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