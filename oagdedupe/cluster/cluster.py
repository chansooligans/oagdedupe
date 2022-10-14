from dataclasses import dataclass
from typing import List, Union

import networkx as nx
import pandas as pd
from dependency_injector.wiring import Provide

from oagdedupe import utils as du
from oagdedupe.base import BaseCluster, BaseCompute
from oagdedupe.containers import Container
from oagdedupe.settings import Settings


@dataclass
class ConnectedComponents(BaseCluster):
    """
    Uses a graph to retrieve connected components
    """

    settings: Settings = Provide[Container.settings]
    compute: BaseCompute = Provide[Container.compute]

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
        scores = self.compute.get_scores(threshold=threshold)
        df_clusters = getattr(self, f"get_connected_components{rl}")(scores)
        self.compute.merge_clusters_with_raw_data(
            df_clusters=df_clusters, rl=rl
        )

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
        g = nx.Graph()
        g.add_weighted_edges_from(
            [
                tuple(
                    [
                        f"{score['_index_l']}",
                        f"{score['_index_r']}",
                        score["score"],
                    ]
                )
                for score in scores.to_dict(orient="records")
            ]
        )
        conn_comp = list(nx.connected_components(g))
        clusters = [
            {"cluster": clusteridx, "_index": int(rec_id), "_type": None}
            for clusteridx, cluster in enumerate(conn_comp)
            for rec_id in cluster
        ]
        return pd.DataFrame(clusters)

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
        g = nx.Graph()
        g.add_weighted_edges_from(
            [
                tuple(
                    [
                        f"{score['_index_l']}_l",
                        f"{score['_index_r']}_r",
                        score["score"],
                    ]
                )
                for score in scores.to_dict(orient="records")
            ]
        )
        conn_comp = list(nx.connected_components(g))
        clusters = [
            {
                "cluster": clusteridx,
                "_index": rec_id.split("_")[0],
                "_type": "_l" in rec_id,
            }
            for clusteridx, cluster in enumerate(conn_comp)
            for rec_id in cluster
        ]
        return pd.DataFrame(clusters)
