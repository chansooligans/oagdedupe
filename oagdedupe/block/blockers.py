from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass

import pandas as pd

from oagdedupe.base import BaseBlocker
from oagdedupe.mixin import BlockerMixin
from oagdedupe.block.groups import Union,Intersection, Pair
from oagdedupe.block.algos import (
    FirstLetter,
    FirstLetterLastToken
)
from oagdedupe.base import BaseBlockAlgo

@dataclass
class PairBlock(BlockerMixin):
    attribute: str
    BlockAlgo: BaseBlockAlgo

@dataclass
class TestBlocker(BaseBlocker, BlockerMixin):
    """
    Blocking configuration for testing.
    Blocking method for all columns is first letter
    """

    @property
    def config(self):
        return Union(
            intersections = [
                    Intersection([
                        Pair(BlockAlgo=FirstLetter(),attribute="name"),
                        Pair(BlockAlgo=FirstLetter(),attribute="addr")
                    ]),
                    Intersection([
                        Pair(BlockAlgo=FirstLetter(),attribute="name"),
                        Pair(BlockAlgo=FirstLetterLastToken(),attribute="name"),
                    ])
                ]
        )

    def get_block_maps(self, df:pd.DataFrame) -> Dict[tuple,tuple]:
        """
        needs work

        For each Intersection:
            For each Pair:
                Get Pair Blocks
                Group Pair Block by Block ID and Aggregate Record IDs

        Returns Intersection Blocks: Combine Pair Blocks by getting unique combinations of Block IDs and sets of Record IDs
        """
        
        block_maps = []
        for intersection in self.config.intersections:
            attribute_blocks = [
                PairBlock(attribute=df[pair.attribute], BlockAlgo=pair.BlockAlgo).get_attribute_blocks()
                for pair in intersection.pairs
            ]

            key_list = self.product([item.keys() for item in attribute_blocks])
            block_map = {}
            for keys in key_list:
                block_map[tuple(keys)] = tuple(
                    set.intersection(*[block[key] for key,block in zip(keys,attribute_blocks)])
                )
            block_maps.append(block_map)
            
        return block_maps

@dataclass
class NoBlocker(BaseBlocker, BlockerMixin):
    """
    No Blocking, gets all combinations.
    """

    def get_block_maps(self, df):
        return [{"_":[x for x in range(len(df))]}]

@dataclass
class ManualBlocker(BaseBlocker, BlockerMixin):

    def get_block_maps(self):
        return

@dataclass
class AutoBlocker(BaseBlocker, BlockerMixin):
    
    def get_block_maps(self):
        return