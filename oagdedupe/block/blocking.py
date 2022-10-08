import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
from dependency_injector.wiring import Provide
from sqlalchemy import create_engine

from oagdedupe._typing import ENGINE, StatsDict
from oagdedupe.base import BaseBlocking
from oagdedupe.block.base import BaseConjunctions, BaseForward, BasePairs
from oagdedupe.block.mixin import ConjunctionMixin
from oagdedupe.containers import Container
from oagdedupe.settings import Settings


@dataclass
class Blocking(BaseBlocking, ConjunctionMixin):
    """
    General interface for blocking:
    - forward: constructs forward indices
    - conjunctions: learns best conjunctions
    - pairs: generates pairs from inverted indices
    """

    forward: BaseForward = None
    conj: BaseConjunctions = None
    pairs: BasePairs = None
    settings: Settings = Provide[Container.settings]

    def _check_rr(self, stats: StatsDict) -> bool:
        """
        check if new block scheme is below minium reduction ratio
        """
        return stats.rr < self.min_rr

    def truncate_table(self, table: str, engine: ENGINE) -> None:
        engine.execute(
            f"""
            TRUNCATE TABLE {self.settings.db.db_schema}.{table};
        """
        )

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
        self.truncate_table(self.comptab_map[table], engine=engine)
        stepsize = n_covered // 10
        step = 0
        for stats in self.conj.conjunctions_list:
            if self._check_rr(stats):
                logging.warning(
                    f"""
                    next conjunction exceeds reduction ratio limit;
                    stopping pair generation with scheme {stats.scheme}
                """
                )
                return
            if table == "blocks_df":
                self.forward.build_forward_indices_full(stats.scheme, engine)
            n_pairs = self.pairs.add_new_comparisons(stats, table, engine)
            if n_pairs // stepsize > step:
                logging.info(f"""{n_pairs} comparison pairs gathered""")
                step = n_pairs // stepsize
            if n_pairs > n_covered:
                return

    def save(self, engine: ENGINE, full: bool = False):

        if full:
            self.forward.init_forward_index_full(engine=engine)
            self.save_comparisons(
                table="blocks_df",
                n_covered=self.settings.model.n_covered,
                engine=engine,
            )
        else:
            self.forward.build_forward_indices(engine=engine)
            self.save_comparisons(
                table="blocks_train", n_covered=500, engine=engine
            )
