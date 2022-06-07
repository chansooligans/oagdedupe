from typing import List, Union, Any, Set, Optional, Dict, Tuple
from dataclasses import dataclass
from functools import cached_property

import pandas as pd

from oagdedupe.base import BaseBlocker
from oagdedupe.mixin import BlockerMixin
from oagdedupe.block.groups import Union, Intersection, Pair
from oagdedupe.block.algos import FirstLetter, FirstLetterLastToken
from oagdedupe.base import BaseBlockAlgo
from oagdedupe.utils import timing

@dataclass
class PairBlock(BlockerMixin):
    """
    Block Analyzer for Attribute-Method Pairs
    """
    v: pd.Series
    BlockAlgo: BaseBlockAlgo

    @cached_property
    def blocks(self) -> List[Tuple[int, str]]:
        """Uses blocking algo on attributes to get list of tuples 
        containing idx and block
        """
        return [(i, self.BlockAlgo.get_block(x)) for i, x in enumerate(self.v.tolist())]

    def get_attribute_blocks(self) -> Dict[str, Set[int]]:
        """converts blocks to dictionary where keys are blocks and values are 
        set of unique idx
        """
        attribute_blocks = {}
        for _id, block in self.blocks:
            attribute_blocks.setdefault(block, set()).add(_id)

        return attribute_blocks


@dataclass
class IntersectionBlock(BlockerMixin):
    """
    Analyzer for intersection of blocks
    """
    df: pd.DataFrame
    intersection: Intersection

    @cached_property
    def blocks(self) -> List[PairBlock]:
        """Create PairBlock for each pair in intersection configuration
        """
        return [
            PairBlock(
                v=self.df[pair.attribute], BlockAlgo=pair.BlockAlgo
            ).get_attribute_blocks()
            for pair in self.intersection.pairs
        ]

    def block_maps(self) -> Dict[tuple, tuple]:
        """Merges a list of PairBlock dictionaries by getting the intersection of keys.
        Then getting the union of values (indices).
        """

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
    def config(self) -> Union:
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

    @timing
    def get_block_maps(self, df) -> List[dict]:
        "returns list of intersection block maps"
        return [
            IntersectionBlock(df=df, intersection=intersection).block_maps()
            for intersection in self.config.intersections
        ]


@dataclass
class NoBlocker(BaseBlocker, BlockerMixin):
    """
    No Blocking, gets all combinations.
    """

    def get_block_maps(self, df) -> List[dict]:
        return [{"_": [x for x in range(len(df))]}]


@dataclass
class ManualBlocker(BaseBlocker, BlockerMixin):
    def get_block_maps(self):
        return


@dataclass
class AutoBlocker(BaseBlocker, BlockerMixin):
    def get_block_maps(self):
        return
