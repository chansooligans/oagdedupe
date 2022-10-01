"""This module contains objects used to construct learn the best
block scheme conjunctions and uses these to generate comparison pairs.
"""

import logging
from functools import cached_property, lru_cache
from multiprocessing import Pool
from typing import Dict, List, Tuple, Union

import tqdm
from typing_extensions import TypedDict

from oagdedupe.block import Blocker
from oagdedupe.block.sql import LearnerSql
from oagdedupe.settings import Settings

StatsType = TypedDict(
    "StatsType",
    {
        "n_pairs": int,
        "positives": int,
        "negatives": int,
        "scheme": Tuple[str],
        "rr": float,
    },
)


class InvertedIndex:
    """
    Used to build inverted index. An inverted index is dataframe
    where keys are signatures and values are arrays of entity IDs
    """

    db: LearnerSql

    def get_stats(self, names: Tuple[str], table: str, rl="") -> StatsType:
        """
        Given forward index, compute stats for comparison pairs
        generated using blocking scheme's inverted index.

        Parameters
        ----------
        names : tuple
            tuple of block schemes
        table : str
            table name of forward index

        Returns
        ----------
        dict
            returns statistics evaluating the conjunction:
                - reduction ratio
                - percent of positive pairs blocked
                - percent of negative pairs blocked
                - number of pairs generated
        """
        res = self.db.get_inverted_index_stats(names=names, table=table)
        res["scheme"] = names
        res["rr"] = 1 - (res["n_pairs"] / (self.db.n_comparisons))
        return res


class DynamicProgram(InvertedIndex):
    """
    Given a block scheme, use dynamic programming algorithm getBest()
    to construct best conjunction
    """

    db: LearnerSql
    settings: Settings

    @lru_cache
    def score(self, arr: Tuple[str]) -> StatsType:
        """
        Wraps get_stats() function with @lru_cache decorator for caching.

        Parameters
        ----------
        arr: tuple
            tuple of block schemes
        """
        return self.get_stats(names=arr, table="blocks_train")

    def _keep_if(self, x: StatsType) -> bool:
        """
        filters for block scheme stats
        """
        return (
            (x["positives"] > 0)
            & (x["rr"] < 1)
            & (x["n_pairs"] > 1)
            & (sum(["_ngrams" in _ for _ in x["scheme"]]) <= 1)
        )

    def _max_key(self, x: StatsType) -> Tuple[float, int, int]:
        """
        block scheme stats ordering
        """
        return (x["rr"], x["positives"], -x["negatives"])

    def _filter_and_sort(
        self, dp: List[StatsType], n: int, scores: List[StatsType]
    ):
        """
        apply filters and sort block schemes
        """
        filtered = [x for x in scores if self._keep_if(x)]
        dp[n] = max(filtered, key=self._max_key)
        return dp

    def get_best(self, scheme: Tuple[str]) -> List[StatsType]:
        """
        Dynamic programming implementation to get best conjunction.

        Parameters
        ----------
        scheme: tuple
            tuple of block schemes
        """
        dp = [
            None for _ in range(self.settings.other.k)
        ]  # type: List[StatsType]
        dp[0] = self.score(scheme)

        if (dp[0]["positives"] == 0) or (dp[0]["rr"] < 0.99):
            return None

        for n in range(1, self.settings.other.k):
            scores = [
                self.score(tuple(sorted(dp[n - 1]["scheme"] + x)))
                for x in self.db.blocking_schemes
                if x not in dp[n - 1]["scheme"]
            ]
            if len(scores) == 0:
                return dp[:n]
            dp = self._filter_and_sort(dp, n, scores)
        return dp


class Conjunctions(DynamicProgram):
    """
    For each block scheme, get the best block scheme conjunctions of
    lengths 1 to k using greedy dynamic programming approach.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.db = LearnerSql(settings=self.settings)

    @property
    def _conjunctions(self) -> List[List[StatsType]]:
        """
        Computes conjunctions for each block scheme in parallel
        """
        with Pool(self.settings.other.cpus) as p:
            res = list(
                tqdm.tqdm(
                    p.imap(self.get_best, self.db.blocking_schemes),
                    total=len(self.db.blocking_schemes),
                )
            )
        return res

    @cached_property
    def conjunctions_list(self) -> List[StatsType]:
        """
        attribute containing list of conjunctions;
        sorted by reduction ratio, positive coverage, negative coverage

        Returns
        ----------
        List[dict]
        """
        # flatten
        res = sum([sublist for sublist in self._conjunctions if sublist], [])
        # dedupe
        res = [dict(t) for t in {tuple(d.items()) for d in res}]
        # sort
        res = sorted(res, key=self._max_key, reverse=True)
        return res

    def _check_rr(self, stats: StatsType) -> bool:
        """
        check if new block scheme is below minium reduction ratio
        """
        return stats["rr"] < self.db.min_rr

    def _add_new_comparisons(self, stats: StatsType, table: str) -> int:
        """
        Computes pairs for conjunction and appends to comparisons or
        full_comparisons table.

        When training on sample, forward indices are pre-computed;
        But for full data, forward indices construction can be expensive,
        so they are computed here as needed.

        Parameters
        ----------
        df: pd.DataFrame
            comparisons pairs already gathered
        stats: dict
            stats for new block scheme
        table: str
            table used to get pairs (either blocks_train for sample or
            blocks_df for full df)

        Returns
        ----------
        int
            total number of pairs gathered so far
        """
        if table == "blocks_df":
            self.blocker.build_forward_indices_full(stats["scheme"])
        self.db.save_comparison_pairs(names=stats["scheme"], table=table)
        n = self.db.get_n_pairs(table=table)
        return n

    def save_comparisons(self, table: str, n_covered: int) -> None:
        """
        Iterates through best conjunction from best to worst.

        For each conjunction, append comparisons to "comparisons"
        or "full_comparisons" (if using full data).

        Stop if (a) subsequent conjunction yields a reduction ratio
        below the minimum rr setting or (b) the number of comparison
        pairs gathered exceeds n_covered.

        Parameters
        ----------
        table: str
            table used to get pairs (either blocks_train for sample or
            blocks_df for full df)
        n_covered: int
            number of records that the conjunctions should cover
        """
        # define here to avoid engine pickle error with multiprocess
        self.blocker = Blocker(settings=self.settings)
        self.db.truncate_table(self.db.comptab_map[table])
        stepsize = n_covered * 0.1
        step = 0
        for stats in self.conjunctions_list:
            if self._check_rr(stats):
                logging.warning(
                    f"""
                    next conjunction exceeds reduction ratio limit;
                    stopping pair generation with scheme {stats["scheme"]}
                """
                )
                return
            n_pairs = self._add_new_comparisons(stats, table)
            if n_pairs // stepsize > step:
                logging.info(f"""{n_pairs} comparison pairs gathered""")
                step = n_pairs // stepsize
            if n_pairs > n_covered:
                return
