from typing import List, Union, Any, Set, Optional, Dict
from dataclasses import dataclass
import itertools
import numpy as np

@dataclass
class BlockerMixin:
    """
    Common operations on blocks and their block maps
    """

    def product(self, lists, nodupes=False) -> List:
        """cartesian product of all vectors in lists"""
        result = [[]]
        for item in lists:
            if nodupes == True:
                result = [x+[y] for x in result for y in item if x != [y]]
            else:
                result = [x+[y] for x in result for y in item]
        return result

    def dedupe_get_candidates(self, block_maps) -> np.array:
        """dedupe: convert union (list of block maps) to candidate pairs

        returns a Nx2 array containing candidate pairs
        """
        return np.unique(
            [
                x
                for block_map in block_maps
                for ids in block_map.values()
                for x in itertools.combinations(ids, 2)
            ],
            axis=0
        )

    def joint_keys(self, dict1, dict2):
        return [name for name in set(dict1).intersection(set(dict2))]

    def rl_get_candidates(self, block_maps1, block_maps2) -> np.array:
        """record linkage: convert union (list of block maps) to candidate pairs;
        unlike dedupe, rl uses block map from df1 and df2, so get candidate pairs
        only where block key exists in both block maps

        returns a Nx2 array containing candidate pairs where first column 
        contains idx for df1 and second column contains idx for df2
        """
        return np.unique(
            [
                tuple(pair)
                for block_map1, block_map2 in zip(block_maps1, block_maps2)
                for key in self.joint_keys(block_map1, block_map2)
                for pair in self.product(
                    [block_map1[key], block_map2[key]], nodupes=False
                )
            ],
            axis=0
        )

@dataclass
class DistanceMixin:
    """
    Mixin class for all distance computers
    """

    def get_distmat(self, df, attributes, indices) -> np.array:
        """for each candidate pair and attribute, compute distances"""
        return np.array([
            [
                self.distance(df[attribute][idx], df[attribute].iloc[idy])
                for attribute in attributes
            ]
            for idx,idy in indices
        ])

    def get_distmat_rl(self, df, df2, attributes, attributes2, indices) -> np.array:
        """record linkage: for each candidate pair and attribute, compute distances"""
        return np.array([
            [
                self.distance(df[attributex].iloc[idx], df2[attributey].iloc[idy])
                for attributex, attributey in zip(attributes,attributes2)
            ]
            for idx,idy in indices
        ])