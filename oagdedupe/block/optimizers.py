"""This module contains objects used to construct learn the best
block scheme conjunctions and uses these to generate comparison pairs.
"""

from dataclasses import dataclass
from functools import lru_cache
from typing import List, Optional, Tuple

from oagdedupe._typing import StatsDict
from oagdedupe.block.base import BaseOptimizer
from oagdedupe.block.schemes import BlockSchemes
from oagdedupe.db.base import BaseRepositoryBlocking
from oagdedupe.settings import Settings


@dataclass
class DynamicProgram(BaseOptimizer, BlockSchemes):
    """
    Given a block scheme, use dynamic programming algorithm getBest()
    to construct best conjunction

    Attributes
    ----------
    repo: BaseRepositoryBlocking
    settings: Settings
    """

    repo: BaseRepositoryBlocking
    settings: Settings

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return hash(id(self))

    @lru_cache
    def score(self, conjunction: Tuple[str]) -> StatsDict:
        """
        Wraps get_conjunction_stats() function with @lru_cache decorator
        for caching.

        Parameters
        ----------
        conjunction: tuple
            tuple of block schemes
        """
        return self.repo.get_conjunction_stats(
            conjunction=conjunction, table="blocks_train"
        )

    def _keep_if(self, x: StatsDict) -> bool:
        """
        filters for block scheme stats
        """
        return (
            (x.positives > 0)
            & (x.rr < 1)
            & (x.n_pairs > 1)
            & (sum(["_ngrams" in _ for _ in x.conjunction]) <= 1)
        )

    def _filter_and_sort(
        self, dp: List[StatsDict], n: int, scores: List[StatsDict]
    ):
        """
        apply filters and sort block schemes
        """
        filtered = [x for x in scores if self._keep_if(x)]
        dp[n] = max(filtered, key=self.repo.max_key)
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
        dp[0] = self.score(conjunction=scheme)

        if (dp[0].positives == 0) or (dp[0].rr < 0.99):
            return None

        for n in range(1, self.settings.model.k):
            scores = [
                self.score(conjunction=tuple(sorted(dp[n - 1].scheme + x)))
                for x in self.block_scheme_tuples
                if x not in dp[n - 1].conjunction
            ]
            if len(scores) == 0:
                return dp[:n]
            dp = self._filter_and_sort(dp, n, scores)
        return dp
