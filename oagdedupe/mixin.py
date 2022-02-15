from typing import List, Union, Any, Set, Optional, Dict
from dataclasses import dataclass
import itertools


@dataclass
class BlockerMixin:
    def product(self, lists, nodupes=False):
        result = [[]]
        for item in lists:
            if nodupes == True:
                result = [x+[y] for x in result for y in item if x != [y]]
            else:
                result = [x+[y] for x in result for y in item]
        return result

    def dedupe_get_candidates(self, block_maps):
        return set(
            [
                x
                for block_map in block_maps
                for ids in block_map.values()
                for x in itertools.combinations(ids, 2)
            ]
        )

    def joint_keys(self, dict1, dict2):
        return [name for name in set(dict1).intersection(set(dict2))]

    def rl_get_candidates(self, block_maps1, block_maps2):
        return set(
            [
                tuple(pair)
                for block_map1, block_map2 in zip(block_maps1, block_maps2)
                for key in self.joint_keys(block_map1, block_map2)
                for pair in self.product(
                    [block_map1[key], block_map2[key]], nodupes=False
                )
            ]
        )
