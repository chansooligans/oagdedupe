from functools import lru_cache, cached_property
import pandas as pd
import itertools
from sqlalchemy import create_engine
from multiprocessing import Pool

def query(sql, engine_url):
    engine = create_engine(engine_url)
    res = pd.read_sql(sql, con=engine)
    engine.dispose()
    return res

class InvertedIndex:
    """
    1. builds block collection (inverted index) where keys are signatures and values are arrays of entity IDs
    2. gets comparison pairs and computes:
        - reduction ratio
        - positive coverage
        - negative coverage
    """

    def check_unnest(self, name):
        if "ngrams" in name:
            return f"unnest({name})"
        return name

    def signatures(self, names):
        return ", ".join([
            f"{self.check_unnest(name)} as signature{i}" 
            for i,name in  enumerate(names)
        ])

    def inverted_index(self, names, table):
        return query(
            f"""
            SELECT 
                {self.signatures(names)}, 
                ARRAY_AGG(_index ORDER BY _index asc)
            FROM {self.schema}.{table}
            GROUP BY {", ".join([f"signature{i}" for i in range(len(names))])}
            """, 
            engine_url=self.engine_url
        )

    def inverted_index_mem(self, names, table):
        df = self.tables[table]
        for x in names:
            if "ngram" in x:
                df = df.explode(x)
        
        # fast way to group:
        arr_slice = df[["_index"]+names].sort_values(names).values
        return pd.DataFrame(
            [
                (key,[_[0] for _ in list(group)]) 
                for key,group in itertools.groupby(arr_slice, lambda x: tuple(x[1:]))
            ],
            columns=["signature","array_agg"]
        )

    def get_pairs(self, names, table, mem):
        """
        get inverted_index then for each array oof entity IDs, get distinct comparison pairs
        """
        if mem == True:
            inverted_index = self.inverted_index_mem(names, table)
        else:
            inverted_index = self.inverted_index(names, table)

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
            self.get_pairs(names=names, table=table, mem=self.settings.other.mem)
            for table in ["blocks_train","blocks_sample"]
        ]

        coverage = self.labels.merge(train_pairs, how = 'left').fillna(0)

        return {
            "scheme": names,
            "rr":1 - (len(sample_pairs) / ((self.n * (self.n-1))/2)),
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

        if (dp[0]["positives"] == 0) or (dp[0]["rr"] < 0.99) or (dp[0]["rr"] == 1):
            return None

        for n in range(1,self.settings.other.k):

            scores = [
                self.score(
                    tuple(sorted(dp[n-1]["scheme"] + [x]))
                ) 
                for x in self.blocking_schemes
                if x not in dp[n-1]["scheme"]
            ]

            scores = [
                x
                for x in scores
                if (x["positives"] > 0) & (x["rr"] < 1)
            ]

            if len(scores) == 0:
                return dp[:n]

            dp[n] = max(
                scores, 
                key=lambda x: (x["rr"], x["positives"], -x["negatives"] -x["n_scheme"])
            )

        return dp


class Coverage(DynamicProgram):

    def __init__(self, settings):
        self.settings = settings
        self.engine_url = f"{settings.other.path_database}"
        self.schema = settings.other.db_schema

    @cached_property
    def blocking_schemes(self):
        """
        get all blocking schemes
        """
        return query(
            f"SELECT * FROM {self.schema}.blocks_train LIMIT 1",
            engine_url=self.engine_url
        ).columns[1:]

    @cached_property
    def n(self):
        """
        sample_n used for reduction ratio computation
        """
        return len(query(
            f"SELECT * FROM {self.schema}.sample",
            engine_url=self.engine_url
        ))

    @cached_property
    def tables(self):
        return {
            "blocks_train":query(
                f"SELECT * FROM {self.schema}.blocks_train", 
                engine_url=self.engine_url
            ),
            "blocks_sample":query(
                f"SELECT * FROM {self.schema}.blocks_sample", 
                engine_url=self.engine_url
            )
        }

    @cached_property
    def labels(self):
        return query(
            f"SELECT * FROM {self.schema}.labels", 
            engine_url=self.engine_url
        )

    @cached_property
    def results(self):
        
        p = Pool(10)
        res = p.map(
            self.getBest, 
            [tuple([o]) for o in self.blocking_schemes]
        )
        p.close()
        p.join()
        
        df = pd.concat([
            pd.DataFrame(r)
            for r in res
        ]).reset_index(drop=True)
        return df.loc[df.astype(str).drop_duplicates().index].sort_values("rr", ascending=False)

    def save(self):
        """
        applies "best" blocking conjunctions on "sample" data
        save output to "comparisons"
        """

        engine = create_engine(self.engine_url)

        schemes = self.results.loc[self.results["n_pairs"].cumsum()<100, "scheme"]
        
        comparisons = pd.concat([
            self.get_pairs(names=x, table="blocks_sample", mem=False)  for x in schemes
        ]).drop(["blocked"], axis=1).drop_duplicates()
        
        comparisons.to_sql(
            "comparisons", 
            schema=self.schema,
            if_exists="replace", 
            con=engine,
            index=False
        )
        engine.dispose()