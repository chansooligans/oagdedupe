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
    Use a graph to retrieve connected components
    """

    def __post_init__(self):
        self.orm = DatabaseORM(settings=self.settings)

    def get_df_cluster(self, matches, scores):
        """ convert connected components to dataframe for user friendly output

        returns: pd.DataFrame for Dedupe or pair of pd.DataFrame for RecordLinkage
        """

        df_clusters = self.get_connected_components(matches, scores)

        # reset table
        self.engine.execute(f"""
            TRUNCATE TABLE {self.settings.other.db_schema}.clusters;
        """)

        with self.Session() as session:
            session.bulk_insert_mappings(
                self.Clusters, 
                df_clusters.to_dict(orient='records')
            )
            session.commit()
        
        return self.orm.get_clusters()


    def get_connected_components(self, matches, scores) -> pd.DataFrame:
        """ build graph with "matched" candidate pairs, weighted by p(match)
        this implementation does not consider p(match) to get connected components
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
