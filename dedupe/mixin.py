from typing import List, Union, Any, Set, Optional, Dict
from dataclasses import dataclass
import itertools

import numpy as np
from multiprocessing import Pool
from tqdm import tqdm

from dedupe.utils import timing

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

    def get_comparisons(self, df, df2, attributes, attributes2, indices):
        if df2 is None:
            df2 = df
        if attributes2 is None:
            attributes2 = attributes
        
        return {
            attribute:np.concatenate(
                (
                    np.array(df[[attribute]].iloc[indices[:,0]]),
                    np.array(df2[[attribute2]].iloc[indices[:,1]])
                ),
                axis=1
            )
            for attribute,attribute2 in zip(attributes,attributes2)
        }

    def get_chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]


    def p_distances(self, comparisons):
        
        # try:
        #     # split block_map into chunks for parallel processing
        #     chunksize = 80
        #     comparisons_split = self.get_chunks(lst=comparisons, n=chunksize)
        #     n_chunks = np.ceil(len(comparisons)/chunksize)
            
        #     # parallel process with progress bar
        #     p = Pool(self.ncores)
        #     results = []

        #     # pmap_chunk is number of chunks sent to each processor at a time and should be multiple of chunksize
        #     pmap_chunk=min(480, int(n_chunks))
        #     for _ in tqdm(p.imap(self.distance, comparisons_split, chunksize=pmap_chunk), total=n_chunks):
        #         results.append(_)
        #         pass
        # except KeyboardInterrupt:
        #     p.terminate()
        #     p.join()
        # else:
        #     p.close()
        #     p.join()
        # if results:
        #     return np.concatenate(results)

        return self.distance(comparisons)

    def get_distmat(self, df, df2, attributes, attributes2, indices) -> np.array:
        """for each candidate pair and attribute, compute distances"""
        
        print(f"making {indices.shape[0]} comparions")
        comparisons = self.get_comparisons(df, df2, attributes, attributes2, indices)

        return np.column_stack([
            self.p_distances(comparisons[attribute])
            for attribute in attributes
        ])
