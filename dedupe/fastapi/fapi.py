from dedupe.labelstudio.lsapi import LabelStudioAPI
from dedupe.block import Blocker,Conjunctions
from dedupe.db.database import Database
from dedupe.db.initialize import Initialize
from dedupe.settings import Settings
from dedupe.db.engine import Engine

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
    
class Projects:
    """
    Fetch Project if it exists; else create new one
    """

    def _check_project_exists(self):
        for project in self.lsapi.list_projects():
            if project.title == self.title:
                return project
        return False

    def _setup_project(self):
        project = self._check_project_exists()
        if not project:
            project = self.lsapi.create_project(
                title=self.title, description=self.description
            )
        return project
    
    def initialize_project(self):
        """
        check for new project; create if not exist
        """
        self.title = self.settings.name
        self.description = self.settings.other.label_studio.description
        self.project = self._setup_project()
        if not self.lsapi.get_webhooks():
            self.lsapi.post_webhook(project_id=self.project.id)

class Tasks:
    """
    Pushes new annotations to label studio if needed.


    """

    def _update_labels(self, df):
        df.to_sql(
            "labels", 
            schema=self.schema, 
            con=self.engine, 
            if_exists="append", 
            index=False
        )

        newtrain = set(df["_index_l"]).union(set(df["_index_r"]))
        self.engine.execute(
            f"""
            INSERT INTO {self.schema}.train 
            SELECT * FROM {self.schema}.df
            WHERE _index IN ({", ".join([str(x) for x in newtrain])})
            """, 
            con=self.engine
        )

    def _get_new_labels(self):
        
        labels = self.lsapi.get_new_labels(project_id=self.project.id)
        
        if len(labels)==0:
            return
        
        tasklist = self.lsapi.get_tasks(project_id=self.project.id)
        newlabels = pd.DataFrame(
            [
                [labels[task.id]] + list(task.data["item"].values())
                for task in tasklist.tasks
                if task.id in labels.keys()
            ],
            columns= ["label"] + self.db.get_compare_cols()
        )

        oldlabels = self.db.get_labels()

        newlabels = newlabels.merge(
            oldlabels[["_index_l","_index_r"]],
            how="left",
            indicator=True
        )

        newlabels = newlabels.loc[
            newlabels["_merge"]=="left_only"
        ].drop(["_merge"], axis=1)

        if len(newlabels) > 0:
            self._update_labels(newlabels)
        else:
            return

    def _get_learning_samples(self, n_instances=5):

        distances = self.db.get_distances()

        sample_idx, _ = self.clf.query(
            distances[self.settings.other.attributes], 
            n_instances=n_instances
        )

        return distances.loc[
            sample_idx,
            self.db.get_compare_cols()
        ]
            
    def _post_tasks(self):
        learning_samples = self._get_learning_samples(n_instances=10)
        self.lsapi.post_tasks(df=learning_samples, project_id=self.project.id)
        return

    def generate_new_samples(self):

        tasklist = self.lsapi.get_tasks(project_id=self.project.id)

        if tasklist.total == 0:
            self._post_tasks()
            return
        
        if tasklist.n_incomplete < 5:
            
            # get new annotations and update
            newlabels = self._get_new_labels()
            if newlabels is None:
                return

            # learn new block conjunctions
            self.init._init_sample()
            self.blocker.build_forward_indices()
            self.cover.save_best()

            # re-train model
            self._train()

            # post new active learning samples to label studio
            self._post_tasks()

class Model(Tasks, Projects, Engine):

    def __init__(self, settings: Settings):
        self.settings = settings
        assert self.settings.other is not None
        assert self.settings.other.path_model is not None
        self.schema = self.settings.other.db_schema

        self.db = Database(settings=settings)
        self.lsapi = LabelStudioAPI(settings=settings)
        self.init = Initialize(settings=self.settings)
        self.blocker = Blocker(settings=self.settings)
        self.coverage = Conjunctions(settings=self.settings)

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
        labels=self.db.get_labels()
        self.clf.teach(labels[self.settings.other.attributes], labels["label"])
        