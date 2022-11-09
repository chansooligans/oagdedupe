from dataclasses import dataclass
from typing import List, Union

import networkx as nx
import pandas as pd

from oagdedupe import utils as du
from oagdedupe.base import BaseCluster
from oagdedupe.db.base import BaseRepository
from oagdedupe.settings import Settings


@dataclass
class ConnectedComponents(BaseCluster):
    """
    Uses a graph to retrieve connected components
    """

    repo: BaseRepository
    settings: Settings

    @du.recordlinkage
    def get_df_cluster(
        self, threshold: float = 0.8, rl: str = ""
    ) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        """
        Convert connected components to dataframe for user friendly output

        Parameters
        ----------
        threshold: float
            pairs below this score are not considered for clustering

        Returns
        ----------
        pd.DataFrame
            clusters merged with raw data
        """
        scores = self.repo.get_scores(threshold=threshold)
        getattr(self, f"get_connected_components{rl}")(scores)
        return self.repo.merge_clusters_with_raw_data(rl=rl)

    def get_connected_components(self, scores: pd.DataFrame) -> pd.DataFrame:
        """
        Build graph with "matched" candidate pairs, weighted by p(match).

        Need to add feature to consider weights when generating
        connected components.

        Parameters
        ----------
        scores: pd.DataFrame
            dataframe with pair indices and match scores

        Returns
        ----------
        pd.DataFrame
            dataframe mapping cluster index to entity index
        """
        self.repo.engine.execute(
            """
        TRUNCATE TABLE dedupe.clusters;
        INSERT INTO dedupe.clusters (cluster, _index, _type)
        SELECT component as cluster, node as _index, Null as _type FROM pgr_connectedComponents(
                'SELECT
                    ROW_NUMBER() OVER (ORDER BY _index_l,_index_r) as id,
                    _index_l as source,
                    _index_r as target,
                    score as cost
                FROM dedupe.scores'
            );
        """
        )

    def get_connected_components_link(
        self, scores: pd.DataFrame
    ) -> pd.DataFrame:
        """
        For record linkage:

        Build graph with "matched" candidate pairs, weighted by p(match).

        Keeps track of whether index is from left or right dataframe

        Need to add feature to consider weights when generating
        connected components.

        Parameters
        ----------
        scores: pd.DataFrame
            dataframe with pair indices and match scores

        Returns
        ----------
        pd.DataFrame
            dataframe mapping cluster index to entity index
        """
        self.repo.engine.execute(
            """
        TRUNCATE TABLE dedupe.clusters;
        INSERT INTO dedupe.clusters (cluster, _index, _type)
        (
            SELECT -1*component as cluster, node as _index, Null as _type FROM pgr_connectedComponents(
                    'SELECT
                        ROW_NUMBER() OVER (ORDER BY _index_l,_index_r) as id,
                        _index_l as source,
                        -1*_index_r as target,
                        score as cost
                    FROM dedupe.scores'
                )
            WHERE component * node < 0
        )
        ;
        """
        )
