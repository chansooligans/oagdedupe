"""This module contains objects used to construct learn the best
block scheme conjunctions and uses these to generate comparison pairs.
"""

from dataclasses import dataclass
from functools import cached_property
from multiprocessing import Pool
from typing import List

import tqdm

from oagdedupe._typing import ENGINE, StatsDict
from oagdedupe.block.base import BaseConjunctions, BaseOptimizer
from oagdedupe.block.schemes import BlockSchemes
from oagdedupe.settings import Settings


@dataclass
class Conjunctions(BaseConjunctions, BlockSchemes):
    """
    For each block scheme, get the best block scheme conjunctions of
    lengths 1 to k using greedy dynamic programming approach.
    """

    optimizer: BaseOptimizer
    settings: Settings

    @property
    def _conjunctions(self) -> List[List[StatsDict]]:
        """
        Computes conjunctions for each block scheme in parallel
        """
        with Pool(self.settings.model.cpus) as p:
            res = list(
                tqdm.tqdm(
                    p.imap(self.optimizer.get_best, self.block_scheme_tuples),
                    total=len(self.block_scheme_tuples),
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
        res = sorted(res, key=self.optimizer.compute.max_key, reverse=True)
        return res
