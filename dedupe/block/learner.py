from dedupe.db.database import Database
from dedupe.settings import Settings
from dedupe.db.engine import Engine

from dataclasses import dataclass
from functools import lru_cache, cached_property
import pandas as pd
import itertools
from sqlalchemy import create_engine
# from pathos.multiprocessing import ProcessingPool as Pool
from multiprocessing.pool import ThreadPool as Pool
import logging

class InvertedIndex:

    def get_pairs(self, names, table):
        """
        inverted index where keys are signatures and values are arrays of entity IDs
        """
        inverted_index = self.db.get_inverted_index(names,table)

        return pd.DataFrame([ 
            y
            for x in list(inverted_index["array_agg"])
            for y in list(itertools.combinations(x, 2))
        ], columns = ["_index_l","_index_r"]).assign(blocked=True).drop_duplicates()

class DynamicProgram(InvertedIndex):
    """
    for each block scheme, get the best conjunctions of lengths 1 to k using greedy approach
    """

    def get_coverage(self, names):
        """
        names:list

        1. get comparisons using train data then merge with labelled data; count 
        how many positive / negative labels are blocked
        2. get comparisons using sample data to compute reduction ratio
        """

        train_pairs, sample_pairs = [
            self.get_pairs(names=names, table=table)
            for table in ["blocks_train","blocks_sample"]
        ]

        coverage = (
            self.db.get_labels()
            .merge(train_pairs, how = 'left')
            .fillna(0)
        )

        n = self.settings.other.n
        n_comparisons = (n * (n-1))/2

        return {
            "scheme": names,
            "rr":1 - (len(sample_pairs) / (n_comparisons)),
            "positives":coverage.loc[coverage["label"]==1, "blocked"].mean(),
            "negatives":coverage.loc[coverage["label"]==0, "blocked"].mean(),
            "n_pairs": len(sample_pairs),
            "n_scheme": len(names)
        }

    @lru_cache
    def score(self, arr):
        """
        arr:tuple
        """
        return self.get_coverage(names=list(arr))

    def getBest(self, scheme):
        """
        dynamic programming implementation to get best conjunction
        """

        dp = [None for _ in range(self.settings.other.k)]
        dp[0] = self.score(scheme)

        if (dp[0]["positives"] == 0) or (dp[0]["rr"] < 0.99) or (dp[0]["rr"] == 1) or (dp[0]["n_pairs"] <= 1):
            return None

        for n in range(1,self.settings.other.k):

            scores = [
                self.score(
                    tuple(sorted(dp[n-1]["scheme"] + [x]))
                ) 
                for x in self.db.blocking_schemes
                if x not in dp[n-1]["scheme"]
            ]

            scores = [
                x
                for x in scores
                if (x["positives"] > 0) & \
                    (x["rr"] < 1) & (x["n_pairs"] > 1) & \
                    (sum(["_ngrams" in _ for _ in x["scheme"]]) <= 1)
            ]
            
            if len(scores) == 0:
                return dp[:n]

            dp[n] = max(
                scores, 
                key=lambda x: (x["rr"], x["positives"], -x["negatives"] -x["n_scheme"])
            )

        return dp

class Conjunctions(DynamicProgram, Engine):

    def __init__(self, settings:Settings):
        self.settings = settings
        self.n = self.settings.other.n
        self.db = Database(settings=self.settings)

    @cached_property
    def results(self):
        
        logging.info(f"getting best conjunctions")
        
        p = Pool(self.settings.other.cpus)
        res = p.map(
            self.getBest, 
            [tuple([o]) for o in self.db.blocking_schemes]
        )
        p.close()
        p.join()
        
        df = pd.concat([
            pd.DataFrame(r)
            for r in res
        ]).reset_index(drop=True)
        return df.loc[
            df.astype(str).drop_duplicates().index
        ].sort_values("rr", ascending=False)

    def best_schemes(self, n_covered):
        return self.results.loc[
            self.results["n_pairs"].cumsum()<n_covered, 
            "scheme"
        ]

    def save_best(
        self, 
        table="blocks_sample", 
        newtable="comparisons",
        n_covered=10
    ):

        newtablemap = {
            "comparisons":self.db.Comparisons,
            "full_comparisons":self.db.FullComparisons
        }

        comparisons = pd.concat([
            self.get_pairs(names=x, table=table)  
            for x in self.best_schemes(n_covered=n_covered)
        ]).drop(["blocked"], axis=1).drop_duplicates()

        with self.db.Session() as session:
            session.bulk_insert_mappings(
                newtablemap[newtable], 
                comparisons.to_dict(orient='records')
            )
            session.commit()
        