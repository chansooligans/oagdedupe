from typing import List, Union, Any, Set, Optional, Dict
from dataclasses import dataclass
import itertools

@dataclass
class CandidatePair:
    idx: int
    idy: int

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
        return [
            (i, self.BlockAlgo.get_block(x)) 
            for i,x in enumerate(self.attribute.tolist())
        ]

    def get_attribute_blocks(self) -> Dict[str, Set[int]]:
        
        attribute_blocks = {}
        for rec_id, block in self.blocks:
            attribute_blocks.setdefault(block, set()).add(rec_id)
        
        return attribute_blocks

    def dedupe_get_candidates(self, block_maps):
        candidates = []
        for block_map in block_maps:
            for x in block_map.values():
                candidates.extend(
                    [
                        pair 
                        for pair in itertools.combinations(x, 2) 
                        if pair not in candidates
                    ]
                )
        for cand in candidates:
            yield cand

    def rl_get_candidates(self, block_maps1, block_maps2):
        candidates_rl = []
        for block_map1, block_map2 in zip(block_maps1, block_maps2):
            joint_keys = [name for name in set(block_map1).intersection(set(block_map2))]
            for key in joint_keys:
                candidates_rl.extend(
                    [
                        pair 
                        for pair in self.product([block_map1[key], block_map2[key]], nodupes=False) 
                        if pair not in candidates_rl
                    ]
                )
        for cand in candidates_rl:
            yield cand
