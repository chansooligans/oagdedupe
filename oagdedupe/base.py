"""
Base classes
"""

from oagdedupe.blocking import union, intersection as bi, utils as blu
from oagdedupe import util as bu
import pandas as pd
from typing import List, Union, Any, Optional, Dict
from functools import cached_property
import networkx as nx
import numpy as np
import logging
from tqdm import tqdm
from datetime import datetime
import itertools
from functools import partial
from multiprocessing import Pool

ClusterField = Union[List[Any], pd.Series, np.ndarray]


class Records:
    """Represents the data of records that need to be deduplicated

    Parameters
    ----------
    rec_id : str
        the name of the field that identifies each record
    df : pd.DataFrame
        the records to be deduplicated
    df2 : Optional[pd.DataFrame]
        for record linkage: second dataframe for record linkage
    true_id : Optional[str]
        the name of the field that truly identifies the entity, if exists (for benchmarking purposes), by default None
    """

    def __init__(
        self,
        rec_id: str,
        df: pd.DataFrame,
        df2: Optional[pd.DataFrame] = None,
        true_id: Optional[str] = None,
    ):
        self.df = df
        self.df2 = df2
        self.recordlink = True if self.df2 is not None else False
        self.rec_id = rec_id
        self.true_id = true_id


class Algo:
    """Represents an entity resolution algorithm

    Parameters
    ----------
    name : str
        name of the algorithm
    threshold : float, optional
        the threshold for a pair of records matching, by default 0.75
    """

    def __init__(self, name: str, threshold: float = 0.75):
        self.name = name
        self.threshold = threshold

    def clear_vars(self, var_names: List[str]) -> None:
        for var_name in var_names:
            if var_name in self.__dict__.keys():
                del self.__dict__[var_name]

    def setup(self, records: Records, cols: List[str]) -> None:
        self.records = records
        self.df = self.records.df
        self.df2 = self.records.df2
        self.rec_id = self.records.rec_id
        self.cols = cols
        self.clear_vars(
            ["matches", "df_clusters", "pred"]
        )  # clear these properties if they were cached for a previous run of __call__

    def __call__(self, records: Records, cols: List[str]) -> ClusterField:
        self.setup(records=records, cols=cols)
        return self.pred

    @cached_property
    def matches(self) -> pd.DataFrame:
        pass

    @cached_property
    def df_clusters(self) -> pd.DataFrame:
        """get the entity clusters from matches

        Returns
        -------
        pd.DataFrame
            columns self.rec_id and cluster
        """
        g = nx.Graph()
        g.add_edges_from([tuple(i) for i in self.matches.values])

        logging.info("getting clusters")
        conn_comp = list(nx.connected_components(g))
        clusters = []
        clustered = set()
        for clusteridx, cluster in enumerate(conn_comp):
            for rec_id in cluster:
                clusters.append({"cluster": clusteridx, self.rec_id: rec_id})
                clustered.add(rec_id)

        # handling unclustered
        logging.info('handling unclustered')
        current_cluster = len(conn_comp)
        if self.df2 is None:
            for rec_id in self.df[self.rec_id].values:
                if not rec_id in clustered:
                    clusters.append(
                        {"cluster": current_cluster, self.rec_id: rec_id}
                    )
                    current_cluster += 1
        
        assert len(clusters) > 0, "no duplicates found"

        print(f'test # of clusters:{len(clusters)}')

        return pd.DataFrame(clusters)

    @cached_property
    def pred(self) -> pd.Series:
        """get predicted clusters as a series

        Returns
        -------
        pd.Series
            predicted cluster for each record
        """
        if self.df2 is not None:
            pred = self.df_clusters.set_index(self.rec_id)
            rec_ids_in_order = list(self.df[self.rec_id].values)
            rec_ids_in_order2 = list(self.df2[self.rec_id].values)
            return [
                pred.loc[pred.index.isin(rec_ids_in_order)]["cluster"],
                pred.loc[pred.index.isin(rec_ids_in_order2)]["cluster"],
            ]

        else:
            pred = self.df_clusters.set_index(
                self.rec_id, 
                verify_integrity=True
            )
            rec_ids_in_order = list(self.df[self.rec_id].values)
            return pred.loc[rec_ids_in_order]["cluster"]

    @property
    def hyperparam(self) -> Dict:
        """some json-like representation of hyperparameters"""
        return {"threshold": self.threshold}


class AlgoWBlocking(Algo):
    def __init__(
        self,
        name: str,
        block_union: union.Union,
        threshold: float = 0.75,
    ):
        super().__init__(name=name, threshold=threshold)
        self.block_union = block_union

    def get_pairs(self, block_map_chunk):
        """ given a chunk of a block_map, return comparison ID pairs for each block_map key"""
        block_map = dict(block_map_chunk)
        blocks = []
        largest_block = 0
        for block_id in block_map.keys():
            if self.df2 is not None:
                pairs = bu.product(
                    [block_map[block_id][0], block_map[block_id][1]],
                    nodupes=False,
                )
            else:
                pairs = itertools.combinations(block_map[block_id], 2)
            
            # avoiding len(pairs) to keep pairs as generator
            n1 = len(blocks)
            blocks += pairs
            n2 = len(blocks)
            largest_block = max(largest_block, n2-n1)

        return [pd.DataFrame(blocks).drop_duplicates().values, largest_block]

    def get_chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def blocks_col(self, intersection: bi.Intersection) -> List[pd.DataFrame]:
        """given a column, separates the column into blocks;
        blocks contain pairwise comparisons of records sharing same block_id

        Parameters
        ----------
        col : str
            column to block

        Returns
        -------
        List
            a list of blocks, each containing pairwise comparisons of records sharing same block_id
        """
        logging.info(f"getting blocks for intersection: {intersection}")

        block_map = blu.get_block_map(self.df, rec_id = self.rec_id, intersection=intersection)
        if self.df2 is not None:
            block_map2 = blu.get_block_map(
                self.df2, rec_id=self.rec_id, intersection=intersection
            )
            block_ids = block_map.keys().__and__(block_map2.keys())
            block_map = {key:[block_map[key],block_map2[key]] for key in block_ids}

        logging.info(len(block_map.keys()))
        logging.info(f"getting candidate pair ids")

        if len(block_map.keys()) > 0:
            try:
            
                # split block_map into chunks for parallel processing
                chunksize = 80
                block_map_split = self.get_chunks(lst=list(block_map.items()), n=chunksize)
                n_chunks = np.ceil(len(list(block_map.items()))/chunksize)
                
                # parallel process with progress bar
                p = Pool(6)
                results = []

                # pmap_chunk is number of chunks sent to each processor at a time
                # should be multiple of chunksize
                pmap_chunk=min(480, int(n_chunks))
                for _ in tqdm(p.imap(self.get_pairs, block_map_split, chunksize=pmap_chunk), total=n_chunks):
                    results.append(_)
                    pass
                
                # get results + print largest block size for debugging
                blocks = [x[0] for x in results]
                pairs = [x for sublist in blocks for x in sublist]
                largest_block = max([x[1] for x in results])
                print(f"\033[1;33m largest block was: {largest_block} \033[0m")

            except KeyboardInterrupt:
                p.terminate()
                p.join()
            else:
                p.close()
                p.join()
        else:
            pairs = []

        return pairs

    @cached_property
    def blocks_all(self) -> List[pd.DataFrame]:
        """gets all blocks for each column

        Returns
        -------
        List
            a list where each element contains a column, represented as a list of blocks
        """
        logging.info("getting all blocks")

        blocks_all = [
            pd.DataFrame(
                self.blocks_col(intersection=intersection)
            ).drop_duplicates()
            for intersection in self.block_union.intersections
        ]

        return pd.concat(blocks_all).drop_duplicates()

    @cached_property
    def candidates(self) -> pd.DataFrame:

        assert len(self.blocks_all) > 0, "no duplicates found"

        candidates = (
            self.df.merge(
                self.blocks_all,
                left_on=self.rec_id,
                right_on=0,
                #validate="one_to_many",
            )
            .merge(
                self.df if self.df2 is None else self.df2,
                left_on=1,
                right_on=self.rec_id,
                suffixes=['_A', '_B'],
                #validate="many_to_one",
            )
            .drop([0, 1], axis=1)
        )

        logging.info(f"making {candidates.shape[0]} comparisons")

        return candidates

    @property
    def hyperparam(self) -> Dict:
        """some json-like representation of hyperparameters"""
        return {
            "threshold": self.threshold
            # "blocking_method": self.block_method,
        }
