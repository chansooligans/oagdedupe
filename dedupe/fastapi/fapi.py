from oagdedupe.labelstudio.lsapi import LabelStudioAPI
from oagdedupe.db.database import Engine
from oagdedupe.settings import Settings
from oagdedupe.api import Dedupe

from dataclasses import dataclass
from typing import List
from modAL.models import ActiveLearner
from modAL.uncertainty import uncertainty_sampling
from sklearn.ensemble import RandomForestClassifier
import requests
import joblib
import pandas as pd
import logging
from sqlalchemy import select, update

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

    def _update_train(self, newlabels):
        indices = set(newlabels["_index_l"]).union(set(newlabels["_index_r"]))
        with self.api.init.Session() as session:
            stmt = (
                update(self.api.init.Train).
                where(self.api.init.Train._index.in_(indices)).
                values(labelled=True)
            )
            session.execute(stmt)
            session.commit()
    
    def _update_labels_and_train_tables(self, newlabels):
        self.api.orm._update_table(newlabels, self.api.init.Labels())
        self._update_train(newlabels)
        return True

    def _get_new_labels(self):
        return self.lsapi.get_new_labels(project_id=self.project.id)

    def _get_learning_samples(self, n_instances=5):
        
        logging.info("getting distances")
        distances = self.api.orm.get_distances()

        logging.info("getting active learning samples")
        sample_idx, _ = self.clf.query(
            distances[self.settings.other.attributes].values, 
            n_instances=n_instances
        )

        return distances.loc[
            sample_idx,
            self.api.orm.get_compare_cols()
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
            if len(newlabels) > 0:
                self._update_labels_and_train_tables(newlabels)
            else:
                return

            # learn new block conjunctions
            self.api.initialize(reset=False, resample=True)
            
            # re-train model
            self._train()

            # post new active learning samples to label studio
            self._post_tasks()

@dataclass
class Model(Tasks, Projects, Engine):
    settings:Settings

    def __post_init__(self):
        self.api =  Dedupe(settings=self.settings)
        self.lsapi = LabelStudioAPI(settings=self.settings)

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
        labels=self.api.orm.get_labels()
        self.clf.teach(labels[self.settings.other.attributes].values, labels["label"].values)
        