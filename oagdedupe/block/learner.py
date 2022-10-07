"""This module contains objects used to construct learn the best
block scheme conjunctions and uses these to generate comparison pairs.
"""

import logging
from functools import cached_property
from multiprocessing import Pool
from typing import List

import tqdm

from oagdedupe._typing import ENGINE, StatsDict
from oagdedupe.block.forward import Forward
from oagdedupe.block.mixin import ConjunctionMixin
from oagdedupe.block.optimizers import DynamicProgram
from oagdedupe.block.sql import LearnerSql
from oagdedupe.settings import Settings


class Conjunctions(ConjunctionMixin):
    """
    For each block scheme, get the best block scheme conjunctions of
    lengths 1 to k using greedy dynamic programming approach.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.db = LearnerSql(settings=self.settings)
        self.optimizer = DynamicProgram(settings=settings, db=self.db)

    @property
    def _conjunctions(self) -> List[List[StatsDict]]:
        """
        Computes conjunctions for each block scheme in parallel
        """
        with Pool(self.settings.other.cpus) as p:
            res = list(
                tqdm.tqdm(
                    p.imap(self.optimizer.get_best, self.db.blocking_schemes),
                    total=len(self.db.blocking_schemes),
                )
            )
        return res

    @cached_property
    def conjunctions_list(self) -> List[StatsDict]:
        """
        attribute containing list of conjunctions;
        sorted by reduction ratio, positive coverage, negative coverage

        Returns
        ----------
        List[dict]
        """
        # flatten
        res = sum(
            [sublist for sublist in self._conjunctions if sublist], []
        )  # type: List[StatsDict]
        # dedupe
        res = list(set(res))
        # sort
        res = sorted(res, key=self._max_key, reverse=True)
        return res

    def _check_rr(self, stats: StatsDict) -> bool:
        """
        check if new block scheme is below minium reduction ratio
        """
        return stats.rr < self.db.min_rr

    def _add_new_comparisons(
        self, stats: StatsDict, table: str, engine: ENGINE
    ) -> int:
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
            self.blocker.build_forward_indices_full(stats.scheme, engine)
        self.db.save_comparison_pairs(names=stats.scheme, table=table)
        n = self.db.get_n_pairs(table=table)
        return n

    def save_comparisons(
        self, table: str, n_covered: int, engine: ENGINE
    ) -> None:
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
        self.blocker = Forward(settings=self.settings)
        self.db.truncate_table(self.db.comptab_map[table])
        stepsize = n_covered // 10
        step = 0
        for stats in self.conjunctions_list:
            if self._check_rr(stats):
                logging.warning(
                    f"""
                    next conjunction exceeds reduction ratio limit;
                    stopping pair generation with scheme {stats.scheme}
                """
                )
                return
            n_pairs = self._add_new_comparisons(stats, table, engine)
            if n_pairs // stepsize > step:
                logging.info(f"""{n_pairs} comparison pairs gathered""")
                step = n_pairs // stepsize
            if n_pairs > n_covered:
                return
