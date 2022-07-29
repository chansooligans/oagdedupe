from dedupe.base import BaseCluster

from dataclasses import dataclass
import networkx as nx
import pandas as pd


@dataclass
class ConnectedComponents(BaseCluster):
    """
    Use a graph to retrieve connected components
    """

    def get_df_cluster(self, matches, scores, rl):
        """ convert connected components to dataframe for user friendly output

        returns: pd.DataFrame for Dedupe or pair of pd.DataFrame for RecordLinkage
        """

        df_clusters = self.get_connected_components(matches, scores)
        df_clusters["x"] = df_clusters["id"].str.contains("x")
        df_clusters["id"] = (
            df_clusters["id"]
            .str.replace("x|y", "", regex=True)
            .astype(float)
            .astype(int)
        )

        if not rl:
            return df_clusters[["id", "cluster"]]
        else:
            return [
                df_clusters.loc[df_clusters["x"] == rl_type, ["id", "cluster"]]
                for rl_type in [True, False]
            ]

    def get_connected_components(self, matches, scores) -> pd.DataFrame:
        """ build graph with "matched" candidate pairs, weighted by p(match)
        this implementation does not consider p(match) to get connected components
        """
        g = nx.Graph()
        g.add_weighted_edges_from([
            tuple([f"{match[0]}x", f"{match[1]}y", score]) 
            for match, score in zip(matches, scores)
        ])
        conn_comp = list(nx.connected_components(g))
        clusters = [
            {"cluster": clusteridx, "id": rec_id}
            for clusteridx, cluster in enumerate(conn_comp)
            for rec_id in cluster
        ]
        return pd.DataFrame(clusters)
