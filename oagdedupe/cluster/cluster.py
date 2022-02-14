from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass
from oagdedupe.base import BaseDistance
import networkx as nx

@dataclass
class ConnectedComponents:

    """
    just copied and pasted below from old code for now!!
    """

    def cluster_ids(self, matches, scores, nobs, rl):
        return self.get_connected_components(matches, scores)

    def get_connected_components(self, matches, scores):
        g = nx.Graph()
        g.add_weighted_edges_from([tuple([f"{match[0]}x",f"{match[1]}y",score]) for match,score in zip(matches,scores)])
        self.conn_comp = list(nx.connected_components(g))
        self.clusters = []
        self.clustered = set()
        for clusteridx, cluster in enumerate(self.conn_comp):
            for rec_id in cluster:
                self.clusters.append({"cluster": clusteridx, "id": rec_id})
                self.clustered.add(rec_id)
        return self.clusters
