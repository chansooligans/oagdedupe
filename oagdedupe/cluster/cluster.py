from oagdedupe.base import BaseCluster
from oagdedupe.settings import Settings
from oagdedupe.db.database import DatabaseORM
from oagdedupe import utils as du

from dataclasses import dataclass
import networkx as nx
import pandas as pd


@dataclass
class ConnectedComponents(BaseCluster, DatabaseORM):
    """
    Uses a graph to retrieve connected components
    """
    settings:Settings

    def __post_init__(self):
        self.orm = DatabaseORM(settings=self.settings)

    @du.recordlinkage
    def get_df_cluster(self, matches, scores, rl=""):
        """ 
        Convert connected components to dataframe for user friendly output

        Parameters
        ----------
        matches: np.array
            array containing pairs of indices that were predicted matches
        scores: np.array
            vector of match scores

        Returns
        ----------
        pd.DataFrame
            clusters merged with raw data
        """

        df_clusters = getattr(self,f"get_connected_components{rl}")(matches, scores)

        # reset table
        self.engine.execute(f"""
            TRUNCATE TABLE {self.settings.other.db_schema}.clusters;
        """)

        self.orm.bulk_insert(
            df=df_clusters, to_table=self.Clusters
        )
        
        return getattr(self.orm,f"get_clusters{rl}")()

    def get_connected_components(self, matches, scores) -> pd.DataFrame:
        """ 
        Build graph with "matched" candidate pairs, weighted by p(match).
        
        Need to add feature to consider weights when generating 
        connected components.

        Parameters
        ----------
        matches: np.array
            array containing pairs of indices that were predicted matches
        scores: np.array
            vector of match scores

        Returns
        ----------
        pd.DataFrame
            dataframe mapping cluster index to entity index
        """
        g = nx.Graph()
        g.add_weighted_edges_from([
            tuple([f"{match[0]}", f"{match[1]}", score]) 
            for match, score in zip(matches, scores)
        ])
        conn_comp = list(nx.connected_components(g))
        clusters = [
            {"cluster": clusteridx, "_index": int(rec_id), "_type":None}
            for clusteridx, cluster in enumerate(conn_comp)
            for rec_id in cluster
        ]
        return pd.DataFrame(clusters)

    def get_connected_components_link(self, matches, scores) -> pd.DataFrame:
        g = nx.Graph()
        g.add_weighted_edges_from([
            tuple([f"{match[0]}_l", f"{match[1]}_r", score]) 
            for match, score in zip(matches, scores)
        ])
        conn_comp = list(nx.connected_components(g))
        clusters = [
            {
                "cluster": clusteridx, 
                "_index": rec_id.strip("_")[0], 
                "_type": "_l" in rec_id
            }
            for clusteridx, cluster in enumerate(conn_comp)
            for rec_id in cluster
        ]
        return pd.DataFrame(clusters)

