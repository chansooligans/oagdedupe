from dedupe.base import BaseBlocker
from dedupe.mixin import BlockerMixin
from dedupe.block.groups import Union, Intersection, Pair
from dedupe.block import algos
from dedupe.base import BaseBlockAlgo

from typing import List, Set, Dict, Tuple
from dataclasses import dataclass
from functools import cached_property
from collections import defaultdict
import pandas as pd
import logging
from tqdm import tqdm


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
        return [self.BlockAlgo.get_block(x) for x in self.v.tolist()]


@dataclass
class IntersectionBlock(BlockerMixin):
    """
    Analyzer for intersection of blocks
    """
    df: pd.DataFrame
    intersection: Intersection

    @cached_property
    def pair_blocks(self) -> List[PairBlock]:
        """Create PairBlock for each pair in intersection configuration
        """
        return [
            PairBlock(
                v=self.df[pair.attribute], BlockAlgo=pair.BlockAlgo
            ).blocks
            for pair in self.intersection.pairs
        ]

    def block_maps(self) -> Dict[str, Set[int]]:
        """converts blocks to dictionary where keys are blocks and values are 
        set of unique idx
        """
        logging.info(f"intersection: {repr(self.intersection)}")
        attribute_blocks = defaultdict(set)
        for _id, block in tqdm(enumerate(zip(*self.pair_blocks))):
            attribute_blocks[block].add(_id)
        return attribute_blocks

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
                        Pair(BlockAlgo=algos.FirstNLetters(N=1), attribute=self.attributes[0]),
                        Pair(BlockAlgo=algos.FirstNLetters(N=1), attribute=self.attributes[1]),
                    ]
                ),
                Intersection(
                    [
                        Pair(BlockAlgo=algos.FirstNLetters(N=1), attribute=self.attributes[0]),
                        Pair(BlockAlgo=algos.FirstNLettersLastToken(N=1), attribute=self.attributes[0]),
                    ]
                ),
            ]
        )

    def get_block_maps(self, df, attributes) -> List[dict]:
        "returns list of intersection block maps"
        self.attributes = attributes
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
    user_config: List[List[dict]]

    @property
    def config(self) -> Union:
        return Union(
            intersections=[
                Intersection(
                    [
                        Pair(BlockAlgo=pair[0], attribute=pair[1])
                        for pair in intersection
                    ]
                )
                for intersection in self.user_config
            ]
        )

    def get_block_maps(self, df, attributes) -> List[dict]:
        "returns list of intersection block maps"
        self.attributes = attributes
        return [
            IntersectionBlock(df=df, intersection=intersection).block_maps()
            for intersection in self.config.intersections
        ]


@dataclass
class AutoBlocker(BaseBlocker, BlockerMixin):
    def get_block_maps(self):
        return
