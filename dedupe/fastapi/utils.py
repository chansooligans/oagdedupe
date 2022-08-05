from pydantic import BaseModel
from typing import List

from modAL.models import ActiveLearner
from modAL.uncertainty import uncertainty_sampling
from sklearn.ensemble import RandomForestClassifier

from functools import cached_property
import requests
import joblib 
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import os
import logging

from dedupe.labelstudio.api import LabelStudioAPI
from dedupe import config

class Query(BaseModel):
    query_index: List[int]
    samples: dict

class Annotation(BaseModel):
    action: str
    annotation: dict
    project: dict

class Tasks:

    def generate_new_samples(self):
        
        tasks = self.lsapi.get_tasks(project_id=self.proj["id"])

        if len(tasks["tasks"]) == 0:
            self.post_tasks()
        else:
            n_incomplete = tasks["total"] - tasks["total_annotations"]
            if  n_incomplete < 5:
                # train and get new samples
                self.get_annotations()
                self.post_tasks()

    def post_tasks(self):
        query_index, df = self.get_samples(n_instances=10)
        df["idx"] = query_index
        df[["idx"]].to_sql("query_index", con=self.engine, if_exists="append", index=False)
        self.lsapi.post_tasks(df=df, project_id=self.proj["id"])
        return

    def get_annotations(self):
        annotations = self.lsapi.get_all_annotations(project_id=self.proj["id"])
        if annotations:
            tasks = [
                [annotations[x['id']]] + list(x['data']["item"].values())
                for x in self.lsapi.get_tasks(project_id=self.proj["id"])["tasks"]
                if x["id"] in annotations.keys()
            ]
            df = pd.DataFrame(tasks, columns = ["label"] + self.attributes_l_r + ["idx"])
            df["label"] = df["label"].map(self.label_map)
            self.train(df)
            df.to_sql("labels", con=self.engine, if_exists="append", index=False)

    @property
    def label_map(self):
        return {
            "Match":1,
            "Not a Match":0,
            "Uncertain":2
        }

    def get_samples(self, n_instances=5):

        ignore_idx = list(pd.read_sql("SELECT distinct idx FROM query_index", con=self.engine)["idx"].values)
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

        samples.columns = self.attributes_l_r

        return query_index, samples
    

class Projects:

    def check_project_exists(self):
        for proj in self.lsapi.list_projects()["results"]:
            if proj["title"] == self.title:
                return proj
        return False

    def setup_project(self):
        proj = self.check_project_exists()
        if not proj:
            new_proj = self.lsapi.create_project(title=self.title, description=self.description)
            return new_proj
        return proj

class Database:

    @cached_property
    def X(self):
        return pd.read_sql_query("select * from distances", con=self.engine)

    @cached_property
    def attributes(self):
        return list(
            pd.read_sql_query("select * from df limit 1", con=self.engine)
            .drop("idx",axis=1).columns
        )

    @property
    def attributes_l_r(self):
        return [x+"_l" for x in self.attributes] + [x+"_r" for x in self.attributes]

class Model(Database, Tasks, Projects):

    def __init__(self):

        self.lsapi = LabelStudioAPI()
        
        logging.info(f'reading database: {config.cache_fp}')
        self.engine = create_engine(f"sqlite:///{config.cache_fp}", echo=True)

        if os.path.exists(config.model_fp):
            self.estimator = joblib.load(config.model_fp)
            logging.info(f'reading model: {config.model_fp}')
        else:
            self.estimator = RandomForestClassifier()

        # check for new project; create if not exist
        self.title = config.ls_title
        self.description = config.ls_description
        self.proj = self.setup_project()
        if not self.lsapi.get_webhooks():
            self.lsapi.post_webhook(project_id=self.proj["id"])

    
    @cached_property
    def clf(self):
        clf = ActiveLearner(
            estimator=self.estimator,
            query_strategy=uncertainty_sampling
        )
        clf.teach(np.repeat(1, len(self.attributes)).reshape(1, -1), [1])
        return clf

    def train(self, df):
        self.clf.teach(
            X = self.X.loc[list(df["idx"])],
            y = df["label"]
        )

    

def url_checker(url):
    try:
        get = requests.get(url)
        if get.status_code == 200:
            return True
        else:
            return False
    except Exception as e:
        return False