from pydantic import BaseModel
from typing import List

from modAL.models import ActiveLearner
from modAL.uncertainty import uncertainty_sampling
from sklearn.ensemble import RandomForestClassifier

from functools import cached_property
import joblib 
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import os
import logging


class Query(BaseModel):
    query_index: List[int]
    samples: dict

class Annotation(BaseModel):
    action: str
    annotation: dict
    project: dict

class Model():

    def __init__(
        self, 
        cache_fp:str = "database.db",
        active_model_fp:str = "model.pkl"
        ):

        self.cache_fp = cache_fp
        self.active_model_fp = active_model_fp

        logging.info(f'reading database: {self.cache_fp}')
        self.engine = create_engine(f"sqlite:///{cache_fp}", echo=True)

        self.active_model_fp = active_model_fp
        if os.path.exists(self.active_model_fp):
            self.estimator = joblib.load(self.active_model_fp)
            logging.info(f'reading model: {self.active_model_fp}')
        else:
            self.estimator = RandomForestClassifier()
    
    @cached_property
    def X(self):
        return pd.read_sql_query("select * from distances", con=self.engine)


    @cached_property
    def attributes(self):
        return list(
            pd.read_sql_query("select * from df limit 1", con=self.engine)
            .drop("idx",axis=1).columns
        )

    @cached_property
    def clf(self):
        clf = ActiveLearner(
            estimator=self.estimator,
            query_strategy=uncertainty_sampling
        )
        clf.teach(np.repeat(1, len(self.attributes)).reshape(1, -1), [1])
        return clf

    def train(self):
        df = pd.read_sql("""
            SELECT idx,label FROM labels
            WHERE label in (0, 1)
        """, con=self.engine).drop_duplicates()

        self.clf.teach(
            X = self.X.loc[list(df["idx"])],
            y = df["label"]
        )

    def get_samples(self, n_instances=5):

        ignore_idx = list(pd.read_sql("SELECT distinct idx FROM labels", con=self.engine)["idx"].values)
        X_subset = self.X.drop(ignore_idx, axis=0)
        
        subset_idx, _ = self.clf.query(
            X_subset, 
            n_instances=n_instances
        )

        query_index = X_subset.index[subset_idx]

        samples = pd.read_sql_query(f"""
                WITH samples AS (
                    SELECT * 
                    FROM idxmat
                    WHERE idx IN ({",".join([
                        str(x)
                        for x in query_index
                    ])})
                )
                SELECT 
                    t2.*,
                    t3.*
                FROM samples t1
                LEFT JOIN df t2
                    ON t1.idxl = t2.idx
                LEFT JOIN df t3
                    ON t1.idxr = t3.idx
                """, con=self.engine
            ).drop("idx",axis=1)

        samples.columns = [x+"_l" for x in self.attributes] + [x+"_r" for x in self.attributes]
        samples["label"] = None

        return query_index, samples