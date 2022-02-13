from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass
from oagdedupe.base import BaseDistance
import networkx as nx

@dataclass
class ConnectedComponents:

    """
    just copied and pasted below from old code for now!!
    """

    def get_connected_components(self):
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

    def handle_unclustered
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