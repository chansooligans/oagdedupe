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
from dedupe.settings import Settings

def url_checker(url):
    try:
        get = requests.get(url)
        if get.status_code == 200:
            return True
        else:
            return False
    except Exception as e:
        return False


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
            if n_incomplete < 5:
                # train and get new samples
                self.get_annotations()
                self.post_tasks()

    def post_tasks(self):
        query_index, df = self.get_samples(n_instances=10)
        df["idx"] = query_index
        df[["idx"]].to_sql(
            "query_index", con=self.engine, if_exists="append", index=False
        )
        self.lsapi.post_tasks(df=df, project_id=self.proj["id"])
        return

    def get_annotations(self):
        annotations = self.lsapi.get_all_annotations(project_id=self.proj["id"])
        if annotations:
            tasks = [
                [annotations[x["id"]]] + list(x["data"]["item"].values())
                for x in self.lsapi.get_tasks(project_id=self.proj["id"])["tasks"]
                if x["id"] in annotations.keys()
            ]
            df = pd.DataFrame(tasks, columns=["label"] + self.attributes_l_r + ["idx"])
            df["label"] = df["label"].map(self.label_map)
            df.to_sql("labels", con=self.engine, if_exists="append", index=False)
            self.train()

    @property
    def label_map(self):
        return {"Match": 1, "Not a Match": 0, "Uncertain": 2}

    def get_samples(self, n_instances=5):

        X_subset = (
            self.distances
            # .drop(ignore_idx, axis=0)
        )

        subset_idx, _ = self.clf.query(X_subset, n_instances=n_instances)

        query_index = X_subset.index[subset_idx]

        samples = pd.read_sql_query(
            f"""
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
                """,
            con=self.engine,
        ).drop("idx", axis=1)

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
            new_proj = self.lsapi.create_project(
                title=self.title, description=self.description
            )
            return new_proj
        return proj


class Database:

    def get_labels(self):
        return pd.read_sql(
            "SELECT * FROM labels",
            con=self.settings.other.path_database
        )

    @cached_property
    def distances(self):
        return pd.read_sql_query("select * from distances", con=self.engine)

    @property
    def attributes_l_r(self):
        return [x + "_l" for x in self.settings.other.attributes] + \
            [x + "_r" for x in self.settings.other.attributes]


class Model(Database, Tasks, Projects):
    def __init__(self, settings: Settings):
        self.settings = settings
        assert self.settings.other is not None

        self.lsapi = LabelStudioAPI(settings=self.settings)

        logging.info(f"reading database: {self.settings.other.path_database}")
        self.engine = create_engine(
            self.settings.other.path_database, echo=True
        )

        assert self.settings.other.path_model is not None
        if self.settings.other.path_model.is_file():
            self.estimator = joblib.load(self.settings.other.path_model)
            logging.info(f"reading model: {self.settings.other.path_model}")
        else:
            self.estimator = RandomForestClassifier(self.settings.other.cpus)

        # check for new project; create if not exist
        self.title = self.settings.name
        self.description = self.settings.other.label_studio.description
        self.proj = self.setup_project()
        if not self.lsapi.get_webhooks():
            self.lsapi.post_webhook(project_id=self.proj["id"])
            
        # initialize active learner
        self.clf = ActiveLearner(
            estimator=self.estimator, query_strategy=uncertainty_sampling
        )
        self.train()
    
    def train(self):
        labels=self.get_labels()
        self.clf.teach(labels[[self.settings.other.attributes]], labels["label"])
        