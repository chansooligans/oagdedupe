"""This module contains mixins with common methods in oagdedupe.block
"""

from dataclasses import dataclass
from functools import cached_property
from multiprocessing import Pool
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from dependency_injector.wiring import Provide
from sqlalchemy import create_engine

import oagdedupe.utils as du
from oagdedupe._typing import StatsDict
from oagdedupe.containers import Container
from oagdedupe.settings import Settings


@dataclass
class ConjunctionMixin:
    settings: Settings = Provide[Container.settings]

    def query(self, sql: str) -> pd.DataFrame:
        """
        for parallel implementation, need to create separate engine
        for each process
        """
        engine = create_engine(self.settings.db.path_database)
        res = pd.read_sql(sql, con=engine)
        engine.dispose()
        return res

    @du.recordlinkage_both
    def n_df(self, rl: str = "") -> pd.DataFrame:
        return self.query(
            f"SELECT count(*) FROM {self.settings.db.db_schema}.df{rl}"
        )["count"].values[0]

    @cached_property
    def n_comparisons(self) -> float:
        """number of total possible comparisons"""
        n = self.n_df()
        if not self.settings.model.dedupe:
            return np.product(n)
        return (n * (n - 1)) / 2

    @property
    def min_rr(self) -> float:
        """minimum reduction ratio"""
        reduced = self.n_comparisons - self.settings.model.max_compare
        return reduced / self.n_comparisons

    @cached_property
    def blocking_schemes(self) -> List[Tuple[str]]:
        """
        Get all blocking schemes

        Returns
        ----------
        List[str]
        """
        return [
            tuple([x])
            for x in self.query(
                f"""
                SELECT *
                FROM {self.settings.db.db_schema}.blocks_train LIMIT 1
                """
            )
            .columns[1:]
            .tolist()
        ]

    @du.recordlinkage
    def _pairs_query(self, names: Tuple[str], rl: str = "") -> str:
        if rl == "":
            where = "WHERE t1._index_l < t2._index_r"
        else:
            where = ""
        return f"""
            SELECT _index_l, _index_r
            FROM inverted_index t1
            JOIN inverted_index_link t2
                ON {" and ".join(
                    [f"t1.{s} = t2.{s}" for s in self._aliases(names)]
                )}
            {where}
            GROUP BY _index_l, _index_r
            """

    def check_unnest(self, name):
        if "ngrams" in name:
            return f"unnest({name})"
        return name

    def signatures(self, names):
        return ", ".join(
            [
                f"{self.check_unnest(name)} as signature{i}"
                for i, name in enumerate(names)
            ]
        )

    def _aliases(self, names: Tuple[str]) -> List[str]:
        return [f"signature{i}" for i in range(len(names))]

    def _inv_idx_query(
        self, names: Tuple[str], table: str, col: str = "_index_l"
    ) -> str:
        return f"""
        SELECT
            {self.signatures(names)},
            unnest(ARRAY_AGG(_index ORDER BY _index asc)) {col}
        FROM {self.settings.db.db_schema}.{table}
        GROUP BY {", ".join(self._aliases(names))}
        """

    def _max_key(self, x: StatsDict) -> Tuple[float, int, int]:
        """
        block scheme stats ordering
        """
        return (x.rr, x.positives, -x.negatives)

    @property
    def comptab_map(self) -> Dict[str, str]:
        return {"blocks_train": "comparisons", "blocks_df": "full_comparisons"}
