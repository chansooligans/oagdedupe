"""Contains object to query and manipulate data from postgres
to construct inverted index and comparison pairs.

This module is only used by oagdedupe.block.learner
"""

from dataclasses import dataclass

from oagdedupe._typing import ENGINE, StatsDict
from oagdedupe.block.base import BasePairs
from oagdedupe.db.base import BaseComputeBlocking
from oagdedupe.settings import Settings


@dataclass
class Pairs(BasePairs):
    """
    Computes pairs for conjunctions and appends to comparisons or
    full_comparisons table.
    """

    compute: BaseComputeBlocking
    settings: Settings

    def add_new_comparisons(self, stats: StatsDict, table: str) -> int:
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
        self.compute.save_comparison_pairs(names=stats.scheme, table=table)
        n = self.compute.get_n_pairs(table=table)
        return n
