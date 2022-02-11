"""Naiveblocking algorithm
"""
from typing import List, Any, Dict, Optional
from oagdedupe import base as b
from oagdedupe.blocking import blockmethod as bm, union, utils as blu
import pandas as pd
import jellyfish
import numpy as np
from functools import cached_property
import networkx as nx
import logging
from tqdm import tqdm
from multiprocessing import Pool
from datetime import datetime


class NaiveBlocking(b.AlgoWBlocking):
    """Represents the NaiveBlocking algorithm

    Parameters
    ----------
    threshold : float
        keep linkage if similarity score exceeds threshold
    block_union : union.Union
        the blocking rules used to separate data
    """

    def __init__(
        self,
        block_union: union.Union,
        threshold: float = 0.75,
    ):
        super().__init__(
            name="naiveblocking",
            threshold=threshold,
            block_union=block_union,
        )

    @cached_property
    def matches(self) -> pd.DataFrame:
        """given a block, get similarity scores

        Returns
        -------
        pd.DataFrame
            a dataframe containing pairs of record IDs whose similarity scores exceed threshold
        """

        logging.info("getting matches")
        for col in self.cols:
            self.candidates[f"{col}_similarity"] = [
                jellyfish.jaro_winkler_similarity(x, y)
                for x, y in tqdm(
                    zip(
                        self.candidates[f'{col}_A'],
                        self.candidates[f'{col}_B'],
                    )
                )
            ]

        self.candidates["similarity"] = self.candidates[
            [f"{col}_similarity" for col in self.cols]
        ].mean(axis=1)

        return self.candidates[self.candidates.similarity >= self.threshold][
            [f"{self.rec_id}_A", f"{self.rec_id}_B"]
        ]

    def eval_blocks(self, block_candidate):
        start = datetime.now()
        blocks = self.blocks_col(block_method=block_candidate)
        n = len(self.df) * len(self.df2)
        return [
            ' & '.join(
                [f"{bc[0].__name__}-{bc[1]}" for bc in block_candidate]
            ),
            len(blocks) / n,
            (datetime.now() - start).seconds,
        ]

    def name_and_addr_block_evaluation(
        self, records: b.Records, cols: List[str]
    ) -> pd.DataFrame:
        self.setup(records=records, cols=cols)
        p = Pool(20)
        results = tqdm(
            p.map(self.eval_blocks, blu.name_addr_blocking_candidates)
        )
        p.close()
        return pd.DataFrame(
            results, columns=['name', 'rr', 'time']
        ).sort_values('rr', ascending=False)

    @property
    def hyperparam(self) -> Dict:
        return {
            "threshold": self.threshold,
            # "blocking_method": self.blocking_method,
        }

    def __call__(self, records: b.Records, cols: List[str]) -> pd.Series:
        self.setup(records=records, cols=cols)
        self.clear_vars(var_names=["blocks_all"])
        return self.pred