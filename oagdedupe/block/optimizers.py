"""This module contains objects used to construct learn the best
block scheme conjunctions and uses these to generate comparison pairs.
"""

from dataclasses import dataclass
from functools import lru_cache
from typing import List, Optional, Tuple

from oagdedupe._typing import StatsDict
from oagdedupe.block.base import BaseOptimizer


@dataclass
class DynamicProgram(BaseOptimizer):
    """
    Given a block scheme, use dynamic programming algorithm getBest()
    to construct best conjunction
    """

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return hash(id(self))

    @lru_cache
    def score(self, arr: Tuple[str]) -> StatsDict:
        """
        Wraps scheme_stats() function with @lru_cache decorator for caching.

        Parameters
        ----------
        arr: tuple
            tuple of block schemes
        """
        return self.compute.get_inverted_index_stats(
            names=arr, table="blocks_train"
        )

    def _keep_if(self, x: StatsDict) -> bool:
        """
        filters for block scheme stats
        """
        return (
            (x.positives > 0)
            & (x.rr < 1)
            & (x.n_pairs > 1)
            & (sum(["_ngrams" in _ for _ in x.scheme]) <= 1)
        )

    def _filter_and_sort(
        self, dp: List[StatsDict], n: int, scores: List[StatsDict]
    ):
        """
        apply filters and sort block schemes
        """
        filtered = [x for x in scores if self._keep_if(x)]
        dp[n] = max(filtered, key=self.compute.max_key)
        return dp

    def get_best(self, scheme: Tuple[str]) -> Optional[List[StatsDict]]:
        """
        Dynamic programming implementation to get best conjunction.

        Parameters
        ----------
        scheme: tuple
            tuple of block schemes
        """
        dp = [
            None for _ in range(self.settings.model.k)
        ]  # type: List[StatsDict]
        dp[0] = self.score(scheme)

        if (dp[0].positives == 0) or (dp[0].rr < 0.99):
            return None

        for n in range(1, self.settings.model.k):
            scores = [
                self.score(tuple(sorted(dp[n - 1].scheme + x)))
                for x in self.block_scheme_tuples
                if x not in dp[n - 1].scheme
            ]
            if len(scores) == 0:
                return dp[:n]
            dp = self._filter_and_sort(dp, n, scores)
        return dp
