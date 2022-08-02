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

# assign the customized OpenAPI schema
class Query(BaseModel):
    query_index: List[int]
    samples: dict

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

