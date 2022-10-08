import json
from dataclasses import dataclass
from typing import Callable, Dict, List, Protocol

import pandas as pd
import requests

from oagdedupe._typing import Annotation, Project, Task, TaskList
from oagdedupe.settings import Settings


class SettingsEnabler(Protocol):
    settings: Settings
    url: str
    headers: Dict[str, str]
    get_tasks: Callable


class APIProjects(SettingsEnabler):
    """
    Interface to get/post Projects from LabelStudio
    """

    def list_projects(self) -> List[Project]:
        """
        Returns
        ----------
        List[Project]
        """
        content = requests.get(f"{self.url}/api/projects", headers=self.headers)
        return [
            Project.parse_obj(p) for p in json.loads(content.content)["results"]
        ]

    def create_project(
        self, title: str = "new project", description: str = "description"
    ):
        """
        Parameters
        ----------
        title: str
        description: str

        Returns
        ----------
        Project
        """

        new_project = {
            "title": title,
            "description": description,
            "label_config": """
                <View>
                    <Header value="Table with {key: value} pairs"/>
                    <Table name="table" value="$item"/>
                    <Choices name="choice" toName="table">
                        <Choice value="Match"/>
                        <Choice value="Not a Match"/>
                        <Choice value="Uncertain"/>
                    </Choices>
                </View>
            """,
        }

        resp = requests.post(
            f"{self.url}/api/projects", headers=self.headers, data=new_project
        )

        return Project.parse_obj(json.loads(resp.content))


class APITasks(SettingsEnabler):
    """
    Interface to get/post Tasks from LabelStudio
    """

    def get_tasks(self, project_id: int) -> TaskList:
        """
        Parameters
        ----------
        project_id: int

        Returns
        ----------
        TaskList
        """
        tasklist = json.loads(
            requests.get(
                f"{self.url}/api/tasks",
                headers=self.headers,
                data={"project": project_id},
            ).content
        )
        tasklist["tasks"] = [Task.parse_obj(t) for t in tasklist["tasks"]]
        return TaskList.parse_obj(tasklist)

    def post_tasks(self, df: pd.DataFrame, project_id: int):
        """
        Parameters
        ----------
        df: pd.DataFrame
        project_id: int
        """
        for _, row in df.iterrows():
            resp = requests.post(
                f"{self.url}/api/tasks",
                headers=self.headers,
                data={
                    "project": project_id,
                    "data": json.dumps({"item": row.to_dict()}),
                },
            )
            json.loads(resp.content)


class APIAnnotations(SettingsEnabler):
    """
    Interface to get/post Annotations from LabelStudio
    """

    def _get_annotationlist_for_task(self, task_id: int) -> List[Annotation]:
        """
        Parameters
        ----------
        task_id: int

        Returns
        ----------
        List[Annotation]
        """
        annotationlist = json.loads(
            requests.get(
                f"{self.url}/api/tasks/{task_id}/annotations/",
                headers=self.headers,
            ).content
        )
        if annotationlist:
            annotationlist = [Annotation.parse_obj(a) for a in annotationlist]
        return annotationlist

    def _latest_annotation(
        self, annotationlist: List[Annotation]
    ) -> Annotation:
        """
        Parameters
        ----------
        annotationlist: List[Annotation]

        Returns
        ----------
        Annotation
        """
        return sorted(annotationlist, key=lambda d: d.created_at)[0].label

    def get_new_labels(self, project_id: int) -> pd.DataFrame:
        """
        Parameters
        ----------
        project_id: int

        Returns
        ----------
        pd.DataFrame
        """
        labels = {}
        for task in self.get_tasks(project_id=project_id).tasks:
            annotationlist = self._get_annotationlist_for_task(task_id=task.id)
            if annotationlist:
                labels[task.id] = {
                    **{"label": self._latest_annotation(annotationlist)},
                    **task.data["item"],
                }
        return pd.DataFrame(labels).T


class APIWebhooks(SettingsEnabler):
    """
    Interface to get/post Webhooks from LabelStudio
    """

    def get_webhooks(self):
        resp = requests.get(f"{self.url}/api/webhooks", headers=self.headers)
        return json.loads(resp.content)

    def post_webhook(self, project_id: int):
        """
        Parameters
        ----------
        project_id: int

        Returns
        ----------
        None
        """
        query = {
            "project": project_id,
            "url": f"{self.settings.fast_api.url}/payload",
            "send_payload": False,
            "is_active": True,
            "actions": ["ANNOTATION_CREATED", "ANNOTATION_UPDATED"],
        }
        requests.post(
            f"{self.url}/api/webhooks", headers=self.headers, data=query
        )


@dataclass
class LabelStudioAPI(APIProjects, APITasks, APIAnnotations, APIWebhooks):
    """
    Interface to get/post Projects, Tasks, Annotations,
    Webhooks from LabelStudio
    """

    settings: Settings

    def __post_init__(self):
        self.url: str = self.settings.label_studio.url
        self.headers: Dict[str, str] = {
            "Authorization": f"""Token {self.settings.label_studio.api_key}"""
        }
