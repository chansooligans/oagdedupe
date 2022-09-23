from dedupe.base import BaseCluster
from dedupe.settings import Settings
from dedupe.db.database import DatabaseORM

from dataclasses import dataclass
import networkx as nx
import pandas as pd


@dataclass
class ConnectedComponents(BaseCluster, DatabaseORM):
    settings:Settings
    """
    Uses a graph to retrieve connected components
    """

    def __post_init__(self):
        self.orm = DatabaseORM(settings=self.settings)

    def get_df_cluster(self, matches, scores):
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

        df_clusters = self.get_connected_components(matches, scores)

        # reset table
        self.engine.execute(f"""
            TRUNCATE TABLE {self.settings.other.db_schema}.clusters;
        """)

        self.orm.bulk_insert(
            df=df_clusters, to_table=self.Clusters
        )
        
        return self.orm.get_clusters()


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
            {"cluster": clusteridx, "_index": int(rec_id)}
            for clusteridx, cluster in enumerate(conn_comp)
            for rec_id in cluster
        ]
        return pd.DataFrame(clusters)
