"""This module contains objects used to construct learn the best
block scheme conjunctions and uses these to generate comparison pairs.
"""

from dataclasses import dataclass
from functools import cached_property, lru_cache
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from dependency_injector.wiring import Provide

import oagdedupe.utils as du
from oagdedupe._typing import StatsDict
from oagdedupe.block.base import BaseOptimizer
from oagdedupe.block.mixin import ConjunctionMixin
from oagdedupe.containers import Container
from oagdedupe.settings import Settings


class DynamicProgram(BaseOptimizer, ConjunctionMixin):
    """
    Given a block scheme, use dynamic programming algorithm getBest()
    to construct best conjunction
    """

    settings: Settings = Provide[Container.settings]

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return hash(id(self))

    @du.recordlinkage
    def get_inverted_index_stats(
        self, names: Tuple[str], table: str, rl: str = ""
    ) -> StatsDict:
        """
        Given forward index, construct inverted index.
        Then for each row in inverted index, get all "nC2" distinct
        combinations of size 2 from the array.

        Then compute number of pairs, the positive coverage and negative
        coverage.

        Parameters
        ----------
        names : List[str]
            list of block schemes
        table : str
            table name of forward index

        Returns
        ----------
        StatsDict
        """
        res = (
            self.query(
                f"""
            WITH
                inverted_index AS (
                    {self._inv_idx_query(names, table)}
                ),
                inverted_index_link AS (
                    {self._inv_idx_query(names, table+rl, col="_index_r")}
                ),
                pairs AS (
                    {self._pairs_query(names)}
                ),
                labels AS (
                    SELECT _index_l, _index_r, label
                    FROM {self.settings.db.db_schema}.labels
                )
            SELECT
                count(*) as n_pairs,
                SUM(CASE WHEN t2.label = 1 THEN 1 ELSE 0 END) positives,
                SUM(CASE WHEN t2.label = 0 THEN 1 ELSE 0 END) negatives
            FROM pairs t1
            LEFT JOIN labels t2
                ON t2._index_l = t1._index_l
                AND t2._index_r = t1._index_r
            """
            )
            .fillna(0)
            .loc[0]
            .to_dict()
        )

        res["scheme"] = names
        res["rr"] = 1 - (res["n_pairs"] / (self.n_comparisons))

        return StatsDict(**res)

    @lru_cache
    def score(self, arr: Tuple[str]) -> StatsDict:
        """
        Wraps scheme_stats() function with @lru_cache decorator for caching.

        Parameters
        ----------
        arr: tuple
            tuple of block schemes
        """
        return self.get_inverted_index_stats(names=arr, table="blocks_train")

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
        dp[n] = max(filtered, key=self._max_key)
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
                for x in self.blocking_schemes
                if x not in dp[n - 1].scheme
            ]
            if len(scores) == 0:
                return dp[:n]
            dp = self._filter_and_sort(dp, n, scores)
        return dp
