from typing import List, Union, Any, Set, Optional, Dict
from dataclasses import dataclass

import pandas as pd

from oagdedupe.base import BaseBlocker
from oagdedupe.mixin import BlockerMixin
from oagdedupe.block.groups import Union, Intersection, Pair
from oagdedupe.block.algos import FirstLetter, FirstLetterLastToken
from oagdedupe.base import BaseBlockAlgo


@dataclass
class PairBlock(BlockerMixin):
    v: pd.Series
    BlockAlgo: BaseBlockAlgo

    @property
    def blocks(self):
        return [(i, self.BlockAlgo.get_block(x)) for i, x in enumerate(self.v.tolist())]

    def get_attribute_blocks(self) -> Dict[str, Set[int]]:

        attribute_blocks = {}
        for _id, block in self.blocks:
            attribute_blocks.setdefault(block, set()).add(_id)

        return attribute_blocks


@dataclass
class IntersectionBlock(BlockerMixin):
    df: pd.DataFrame
    intersection: Intersection

    @property
    def blocks(self):
        return [
            PairBlock(
                v=self.df[pair.attribute], BlockAlgo=pair.BlockAlgo
            ).get_attribute_blocks()
            for pair in self.intersection.pairs
        ]

    def block_maps(self):

        key_list = self.product([item.keys() for item in self.blocks])
        block_map = {}
        for keys in key_list:
            block_map[tuple(keys)] = tuple(
                set.intersection(*[block[key] for key, block in zip(keys, self.blocks)])
            )
        return block_map


@dataclass
class TestBlocker(BaseBlocker, BlockerMixin):
    """
    Blocking configuration for testing.
    Blocking method for all columns is first letter
    """

    @property
    def config(self):
        return Union(
            intersections=[
                Intersection(
                    [
                        Pair(BlockAlgo=FirstLetter(), attribute="name"),
                        Pair(BlockAlgo=FirstLetter(), attribute="addr"),
                    ]
                ),
                Intersection(
                    [
                        Pair(BlockAlgo=FirstLetter(), attribute="name"),
                        Pair(BlockAlgo=FirstLetterLastToken(), attribute="name"),
                    ]
                ),
            ]
        )

    def get_block_maps(self, df) -> Dict[tuple, tuple]:
        return [
            IntersectionBlock(df=df, intersection=intersection).block_maps()
            for intersection in self.config.intersections
        ]


@dataclass
class NoBlocker(BaseBlocker, BlockerMixin):
    """
    No Blocking, gets all combinations.
    """

    def get_block_maps(self, df):
        return [{"_": [x for x in range(len(df))]}]


@dataclass
class ManualBlocker(BaseBlocker, BlockerMixin):
    def get_block_maps(self):
        return


@dataclass
class AutoBlocker(BaseBlocker, BlockerMixin):
    def get_block_maps(self):
        return
