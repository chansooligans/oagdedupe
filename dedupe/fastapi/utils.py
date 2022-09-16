from dedupe.labelstudio.lsapi import LabelStudioAPI
from dedupe.block import Blocker, Coverage
from dedupe.settings import Settings

from pydantic import BaseModel
from typing import List
from modAL.models import ActiveLearner
from modAL.uncertainty import uncertainty_sampling
from sklearn.ensemble import RandomForestClassifier
from functools import cached_property
import requests
import joblib
import pandas as pd
from sqlalchemy import create_engine
import logging

def url_checker(url):
    try:
        get = requests.get(url)
        if get.status_code == 200:
            return True
        else:
            return False
    except Exception as e:
        return False

class Annotation(BaseModel):
    action: str
    annotation: dict
    project: dict


class Projects:

    def _check_project_exists(self):
        for proj in self.lsapi.list_projects_from_ls()["results"]:
            if proj["title"] == self.title:
                return proj
        return False

    def _setup_project(self):
        proj = self.check_project_exists()
        if not proj:
            new_proj = self.lsapi.create_project_on_ls(
                title=self.title, description=self.description
            )
            return new_proj
        return proj
    
    def initialize_project(self):
        """
        check for new project; create if not exist
        """
        self.title = self.settings.name
        self.description = self.settings.other.label_studio.description
        self.proj = self.setup_project()
        if not self.lsapi.get_webhooks_from_ls():
            self.lsapi.post_webhook_to_ls(project_id=self.proj["id"])

class Database:

    def get_labels(self):
        return pd.read_sql(
            f"SELECT * FROM {self.schema}.labels",
            con=self.engine
        )

    def get_distances(self):
        return pd.read_sql_query(
            f"""
            SELECT t1.*
            FROM {self.schema}.distances t1
            LEFT JOIN {self.schema}.labels t2
                ON t1._index_l = t2._index_l
                AND t1._index_r = t2._index_r
            WHERE t2._index_l is null
            """, 
            con=self.engine
        )

class Tasks:
    
    @property
    def label_map(self):
        return {"Match": 1, "Not a Match": 0, "Uncertain": 2}

    def _get_learning_samples(self, n_instances=5):

        distances = self.get_distances()

        sample_idx, _ = self.clf.query(
            distances[self.settings.other.attributes], 
            n_instances=n_instances
        )

        return distances.loc[
            sample_idx,
            [f"{x}_l" for x in self.settings.other.attributes] +
            [f"{x}_r" for x in self.settings.other.attributes]
        ]

    def _get_annotations(self):
        annotations = self.lsapi.get_all_annotations_from_ls(project_id=self.proj["id"])
        if annotations:
            tasks = [
                [annotations[x["id"]]] + list(x["data"]["item"].values())
                for x in self.lsapi.get_tasks_from_ls(project_id=self.proj["id"])["tasks"]
                if x["id"] in annotations.keys()
            ]
            df = pd.DataFrame(tasks, columns=["label"] + self.attributes_l_r)
            df["label"] = df["label"].map(self.label_map)
            df.to_sql("labels", con=self.engine, if_exists="append", index=False)
            # !
            # !
            # # ALSO APPEND TO dedupe.train
            # !
            # !

    def _post_tasks(self):
        learning_samples = self._get_learning_samples(n_instances=10)
        self.lsapi.post_tasks_to_ls(df=learning_samples, project_id=self.proj["id"])
        return

    def generate_new_samples(self):
        """
        generates new samples and posts to label studio;
        if the number of tasks is less than X, re-trains models
        """

        tasks = self.lsapi.get_tasks_from_ls(project_id=self.proj["id"])

        if len(tasks["tasks"]) == 0:
            self._post_tasks()
        else:
            n_incomplete = tasks["total"] - tasks["total_annotations"]
            if n_incomplete < 5:
                
                # draw new sample
                self.blocker._init_sample()
                self.cover.save()

                # train and get new samples
                self._get_annotations()
                self._train()
                self._post_tasks()

class Model(Database, Tasks, Projects):

    def __init__(self, settings: Settings):
        self.settings = settings
        assert self.settings.other is not None
        assert self.settings.other.path_model is not None
        self.schema = self.settings.other.db_schema

        self.lsapi = LabelStudioAPI(settings=settings)
        self.blocker = Blocker(settings=self.settings)
        self.coverage = Coverage(settings=self.settings)
    
    @cached_property
    def engine(self):
        logging.info(f"reading database: {self.settings.other.path_database}")
        return create_engine(
            self.settings.other.path_database, echo=True
        )

    def initialize_learner(self):
        """
        initialize active learner
        """
        if self.settings.other.path_model.is_file():
            self.estimator = joblib.load(self.settings.other.path_model)
            logging.info(f"reading model: {self.settings.other.path_model}")
        else:
            self.estimator = RandomForestClassifier(self.settings.other.cpus)

        self.clf = ActiveLearner(
            estimator=self.estimator, query_strategy=uncertainty_sampling
        )
        self._train()
    
    def _train(self):
        labels=self.get_labels()
        self.clf.teach(labels[self.settings.other.attributes], labels["label"])
        