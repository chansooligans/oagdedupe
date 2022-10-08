""" this module contains the API for fastAPI, used to facilitate communication
between postgres, label-studio, the active-learning model, and the
block-learner
"""

import logging
from dataclasses import dataclass
from typing import List, Optional, Protocol

import joblib
import pandas as pd
import requests
from modAL.models import ActiveLearner
from modAL.uncertainty import uncertainty_sampling
from sklearn.ensemble import RandomForestClassifier
from sqlalchemy import update

from oagdedupe._typing import Annotation, Project, Task, TaskList
from oagdedupe.api import Fapi
from oagdedupe.labelstudio.lsapi import LabelStudioAPI
from oagdedupe.settings import Settings


def url_checker(url):
    try:
        get = requests.get(url)
        if get.status_code == 200:
            return True
        else:
            return False
    except Exception as e:
        logging.error(e)
        return False


class SettingsEnabler(Protocol):
    settings: Settings
    api: Fapi
    lsapi: LabelStudioAPI
    clf: ActiveLearner
    project: Project


class Projects(SettingsEnabler):
    """
    Fetch Project if it exists; else create new one
    """

    def _check_project_exists(self) -> Optional[Project]:
        """
        get list of projects from label-studio
        if project with same title exists, return project
        otherwise return None

        Returns
        ----------
        Optional[Project]
        """
        for project in self.lsapi.list_projects():
            if project.title == self.settings.name:
                return project
        return None

    def _setup_project(self) -> Project:
        """
        return project from label-studio if it exists;
        otherwise, create a new project

        Returns
        ----------
        Project
        """
        project = self._check_project_exists()
        if project is None:
            project = self.lsapi.create_project(
                title=self.settings.name,
                description=self.settings.label_studio.description,
            )
        return project

    def initialize_project(self) -> None:
        """
        1) create project if not exist;
        2) post webhooks (sets up webhooks on label-studio, which are
        automated messages label-studio sends to fapi as labelling tasks
        are completed)
        """
        self.project = self._setup_project()
        if not self.lsapi.get_webhooks():
            self.lsapi.post_webhook(project_id=self.project.id)


class TasksPost(SettingsEnabler):
    """
    Pushes new labels to label studio
    """

    def _get_learning_samples(self, n_instances: int = 5) -> pd.DataFrame:
        """
        apply uncertainty sampling with active learning model on
        "distances" table to get new samples for labelling
        """

        logging.info("getting distances")
        distances = self.api.orm.get_distances()

        logging.info("getting active learning samples")
        sample_idx, _ = self.clf.query(
            distances[self.settings.attributes].values,
            n_instances=n_instances,
        )

        return distances.loc[sample_idx, self.api.orm.compare_cols]

    def _post_tasks(self) -> None:
        """
        send new samples to label-studio
        """
        learning_samples = self._get_learning_samples(n_instances=10)
        self.lsapi.post_tasks(df=learning_samples, project_id=self.project.id)


class TasksGet(SettingsEnabler):
    """
    Gets new labels from label studio
    """

    def _update_train(self, newlabels: pd.DataFrame) -> None:
        """
        for entities that were labelled,
        set "labelled" column in train table to True
        """
        indices = set(newlabels["_index_l"]).union(set(newlabels["_index_r"]))
        with self.api.init.Session() as session:
            stmt = (
                update(self.api.init.Train)
                .where(self.api.init.Train._index.in_(indices))
                .values(labelled=True)
            )
            session.execute(stmt)
            session.commit()

    def _update_labels(self, newlabels: pd.DataFrame) -> None:
        """
        add new labels to labels table
        """
        self.api.orm._update_table(newlabels, self.api.init.Labels())

    def _get_new_labels(self) -> pd.DataFrame:
        """
        get new labels from label-studio
        """
        return self.lsapi.get_new_labels(project_id=self.project.id)


@dataclass
class Model(TasksGet, TasksPost, Projects):
    """
    Interface to facilitate communication between postgres, label-studio,
    the active-learning model, and the block-learner
    """

    settings: Settings

    def __post_init__(self):
        self.api = Fapi(settings=self.settings)
        self.lsapi = LabelStudioAPI(settings=self.settings)

    def initialize_learner(self) -> None:
        """
        initialize active learner
        """
        if self.settings.model.path_model.is_file():
            self.estimator = joblib.load(self.settings.model.path_model)
            logging.info(f"reading model: {self.settings.model.path_model}")
        else:
            self.estimator = RandomForestClassifier(self.settings.model.cpus)

        self.clf = ActiveLearner(
            estimator=self.estimator, query_strategy=uncertainty_sampling
        )
        self._train()

    def _train(self) -> None:
        """
        trains active learning model
        """
        labels = self.api.orm.get_labels()
        self.clf.teach(
            labels[self.settings.attributes].values,
            labels["label"].values,
        )

    def generate_new_samples(self) -> None:
        """
        update database with new labels if available and re-train model;
        post uncertainty samples to label-studio for review
        """

        tasklist = self.lsapi.get_tasks(project_id=self.project.id)

        if tasklist.total == 0:
            self._post_tasks()
            return

        if tasklist.n_incomplete < 5:

            # get new annotations and update
            newlabels = self._get_new_labels()
            if len(newlabels) > 0:
                self._update_labels(newlabels)
                self._update_train(newlabels)
            else:
                return

            # learn new block conjunctions
            self.api.initialize(reset=False, resample=True)

            # re-train model
            self._train()

            # post new active learning samples to label studio
            self._post_tasks()
