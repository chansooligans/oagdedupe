from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass



from oagdedupe.base import BaseBlocker
from oagdedupe.mixin import BlockerMixin
from oagdedupe.block.algos import (
    FirstLetter
)

class TestBlocker(BaseBlocker, BlockerMixin):
    """
    Blocking configuration for testing.
    Blcoking method for all columns is first letter
    """

    def get_blocks_for_col(self, df, col):
        blocks = [(i, FirstLetter().get_block(x)) for i,x in enumerate(df[col].tolist())]
        blocks_for_col = {}
        for rec_id, block in blocks:
            blocks_for_col.setdefault(block, set()).add(rec_id)
        return blocks_for_col

    def get_block_map(self, df ,cols):
        blocks1 = self.get_blocks_for_col(df,"name")
        blocks2 = self.get_blocks_for_col(df,"addr")

        block_map = {}
        for key1,key2 in self.product([blocks1.keys(),blocks2.keys()]):
            block_map[(key1,key2)] = tuple(blocks1[key1] & blocks2[key2])
        return block_map

class ManualBlocker(BaseBlocker, BlockerMixin):

    def get_block_map(self):
        return

class AutoBlocker(BaseBlocker, BlockerMixin):
    
    def get_block_map(self):
        return