"""This module contains objects used to construct learn the best
block scheme conjunctions and uses these to generate comparison pairs.
"""

import multiprocessing as mp
from dataclasses import dataclass
from functools import cached_property, partial
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

    Attributes
    ----------
    optimizer: BaseOptimizer
    settings: Settings
    """

    optimizer: BaseOptimizer
    settings: Settings

    @property
    def _conjunctions(self) -> List[List[StatsDict]]:
        """
        Computes conjunctions for each block scheme in parallel

        Returns
        ----------
        List[List[StatsDict]]
        """
        with mp.Manager() as manager:
            shared_dict = manager.dict()
            func = partial(self.optimizer.get_best, shared_dict=shared_dict)
            with manager.Pool(self.settings.model.cpus) as p:
                res = list(
                    tqdm.tqdm(
                        p.imap(func, self.block_scheme_tuples),
                        total=len(self.block_scheme_tuples),
                    )
                )
        return res

    @cached_property
    def conjunctions_list(self) -> List[StatsDict]:
        """
        flattens, dedupes and sorts list of conjunctions

        Returns
        ----------
        List[StatsDict]
        """
        # flatten
        res = sum(
            [sublist for sublist in self._conjunctions if sublist], []
        )  # type: List[StatsDict]
        res = list(set(res))
        # sort
        res = sorted(res, key=self.optimizer.repo.max_key, reverse=True)
        return res
