from typing import List, Union, Any, Set, Optional, Dict
from dataclasses import dataclass
import itertools

@dataclass
class BlockerMixin:
    
    def product(self, lists, nodupes=True):
        result = [[]]
        for item in lists:
            if nodupes==True:
                result = [x+[y] for x in result for y in item if x != [y]]
            else:
                result = [x+[y] for x in result for y in item]
        return result

    @property
    def blocks(self):
        return [(i, self.BlockAlgo.get_block(x)) for i,x in enumerate(self.attribute.tolist())]

    def get_attribute_blocks(self) -> Dict[str, Set[int]]:
        
        attribute_blocks = {}
        for rec_id, block in self.blocks:
            attribute_blocks.setdefault(block, set()).add(rec_id)
        
        return attribute_blocks