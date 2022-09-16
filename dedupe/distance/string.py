from dedupe.base import BaseDistance
from dataclasses import dataclass
from jellyfish import jaro_winkler_similarity
import ray
import numpy as np
import pandas as pd


@ray.remote
def ray_distance(pairs):
    return [
        jaro_winkler_similarity(pair[0], pair[1])
        for pair in pairs
    ]

@dataclass
class DistanceMixin:
    """
    Mixin class for all distance computers
    """

    def get_comparisons(self, engine, attributes, indices):
        df = pd.read_sql("SELECT * FROM dedupe.sample", con=engine)
        df.index = df["_index"]

        return {
            attribute: np.concatenate(
                (
                    np.array(df.loc[indices[:, 0], [attribute]]),
                    np.array(df.loc[indices[:, 1], [attribute]])
                ),
                axis=1
            )
            for attribute in attributes
        }

    def get_chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def get_distmat(self, schema, engine, attributes, indices) -> np.array:
        """for each candidate pair and attribute, compute distances"""

        print(f"making {indices.shape[0]} comparions")
        comparisons = self.get_comparisons(engine, attributes, indices)

        distances = pd.DataFrame(np.column_stack([
            self.distance(comparisons[attribute])
            for attribute in attributes
        ]))
        
        distances.to_sql(
            "distances",
            schema=schema,
            if_exists="replace", 
            con=engine,
            index=False
        )


class RayAllJaro(BaseDistance, DistanceMixin):
    "needs work: update to allow user to specify attribute-algorithm pairs"

    def distance(self, pairs):
        chunks = self.get_chunks(pairs, 10000)
        res = ray.get([
            ray_distance.remote(chunk)
            for chunk in chunks
        ])
        return [
            x for sublist in res for x in sublist
        ]

    def config(self):
        """
        returns dict mapping each column to distance calculation
        """
        return dict

class AllJaro(BaseDistance, DistanceMixin):
    "needs work: update to allow user to specify attribute-algorithm pairs"

    def distance(self, pairs):
        return [
            jaro_winkler_similarity(pair[0], pair[1])
            for pair in pairs
        ]

    def config(self):
        """
        returns dict mapping each column to distance calculation
        """
        return dict
