import logging
from dataclasses import dataclass

from oagdedupe._typing import ENGINE, StatsDict
from oagdedupe.base import BaseBlocking


@dataclass
class Blocking(BaseBlocking):
    """
    General interface for blocking:
    - forward: constructs forward indices
    - conjunctions: learns best conjunctions
    - pairs: generates pairs from inverted indices
    """

    def __post_init__(self):
        self.settings = self.compute.settings
        self.forward = self.forward(
            compute=self.compute, settings=self.settings
        )
        self.pairs = self.pairs(compute=self.compute, settings=self.settings)
        self.conj = self.conj(
            settings=self.settings,
            optimizer=self.optimizer(
                compute=self.compute, settings=self.settings
            ),
        )

    def _check_rr(self, stats: StatsDict) -> bool:
        """
        check if new block scheme is below minium reduction ratio
        """
        return stats.rr < self.forward.compute.min_rr

    def save_comparisons(
        self, table: str, n_covered: int, engine=ENGINE
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
        stepsize = n_covered // 10
        step = 0
        for i, stats in enumerate(self.conj.conjunctions_list):
            if self._check_rr(stats):
                logging.warning(
                    f"""
                    next conjunction exceeds reduction ratio limit;
                    stopping pair generation with scheme {stats.scheme}
                """
                )
                return
            if table == "blocks_df":
                self.forward.build_forward_indices(
                    columns=stats.scheme, engine=engine, iter=i, full=True
                )
            n_pairs = self.pairs.add_new_comparisons(stats, table)
            if n_pairs // stepsize > step:
                logging.info(f"""{n_pairs} comparison pairs gathered""")
                step = n_pairs // stepsize
            if n_pairs > n_covered:
                return

    def save(self, engine: ENGINE, full: bool = False):

        if full:
            self.save_comparisons(
                table="blocks_df",
                n_covered=self.settings.model.n_covered,
                engine=engine,
            )
        else:
            self.forward.build_forward_indices(engine=engine, full=False)
            self.save_comparisons(
                table="blocks_train", n_covered=500, engine=engine
            )
