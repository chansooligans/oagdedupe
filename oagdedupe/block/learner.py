"""This module contains objects used to construct learn the best
block scheme conjunctions and uses these to generate comparison pairs.
"""

from functools import lru_cache, cached_property
import logging
from multiprocessing import Pool
import tqdm

from oagdedupe.block.sql import LearnerSql
from oagdedupe.settings import Settings
from oagdedupe.block import Blocker

    
class InvertedIndex:
    """
    Used to build inverted index. An inverted index is dataframe
    where keys are signatures and values are arrays of entity IDs
    """

    def get_stats(self, names: tuple, table, rl=""):
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
        res = self.db.get_inverted_index_stats(
            names=names,
            table=table
        )
        res["scheme"] = names
        res["rr"] = 1 - (res["n_pairs"] / (self.db.n_comparisons))
        return res


class DynamicProgram(InvertedIndex):
    """
    Given a block scheme, use dynamic programming algorithm getBest()
    to construct best conjunction
    """

    @lru_cache
    def score(self, arr: tuple):
        """
        Wraps get_stats() function with @lru_cache decorator for caching.

        Parameters
        ----------
        arr: tuple
            tuple of block schemes
        """
        return self.get_stats(names=arr, table="blocks_train")

    def _keep_if(self, x):
        """
        filters for block scheme stats
        """
        return (
            (x["positives"] > 0) & \
            (x["rr"] < 1) & \
            (x["n_pairs"] > 1) & \
            (sum(["_ngrams" in _ for _ in x["scheme"]]) <= 1)
        )

    def _max_key(self, x):
        """
        block scheme stats ordering
        """
        return (
            x["rr"], 
            x["positives"], 
            -x["negatives"]
        )

    def _filter_and_sort(self, dp, n, scores):
        """
        apply filters and sort block schemes
        """
        filtered = [
            x
            for x in scores
            if self._keep_if(x)
        ]
        dp[n] = max(filtered, key=self._max_key)
        return dp

    def get_best(self, scheme: tuple):
        """
        Dynamic programming implementation to get best conjunction.

        Parameters
        ----------
        scheme: tuple
            tuple of block schemes
        """
        dp = [None for _ in range(self.settings.other.k)]
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
    def _conjunctions(self):
        """
        Computes conjunctions for each block scheme in parallel
        """
        with Pool(self.settings.other.cpus) as p:
            res = list(tqdm.tqdm(
                p.imap(self.get_best, self.db.blocking_schemes),
                total=len(self.db.blocking_schemes)
            ))
        return res

    @cached_property
    def conjunctions_list(self):
        """
        attribute containing list of conjunctions;
        sorted by reduction ratio, positive coverage, negative coverage

        Returns
        ----------
        List[dict]
        """
        logging.info("getting best conjunctions")   
        # flatten
        res = sum([sublist for sublist in self._conjunctions if sublist], [])
        # dedupe
        res = [dict(t) for t in {tuple(d.items()) for d in res}]
        # sort
        res = sorted(res, key=self._max_key, reverse=True)
        return res

    def _check_rr(self, stats):
        """
        check if new block scheme is below minium reduction ratio
        """
        return stats["rr"] < self.db.min_rr

    def _add_new_comparisons(self, stats, table):
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

    def save_comparisons(
            self,
            table,
            n_covered
    ):
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
        stepsize = n_covered * .1
        step = 0
        for stats in self.conjunctions_list:
            if self._check_rr(stats):
                logging.warning(f"""
                    next conjunction exceeds reduction ratio limit;
                    stopping pair generation with scheme {stats["scheme"]}
                """)
                return 
            n_pairs = self._add_new_comparisons(stats, table)
            if n_pairs // stepsize > step:
                logging.info(f"""{n_pairs} comparison pairs gathered""")
                step = n_pairs // stepsize
            if n_pairs > n_covered:
                return
