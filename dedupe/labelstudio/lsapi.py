from pydantic import BaseModel
from oagdedupe.settings import Settings

from dataclasses import dataclass
from typing import List, Dict, Optional
import requests
import pandas as pd
import json

class Annotation(BaseModel):
    id: int
    created_username: str
    created_ago: str
    result: List[dict]
    was_cancelled: bool
    ground_truth: bool
    created_at: str
    updated_at: str
    lead_time: float
    task: int
    completed_by: int

    @property
    def label_map(self):
        return {"Match": 1, "Not a Match": 0, "Uncertain": 2}

    @property
    def label(self):
        annotation = self.result[0]["value"]["choices"][0]
        return self.label_map[annotation]

class Task(BaseModel):
    id: int
    cancelled_annotations: int
    total_annotations: int
    total_predictions: int
    updated_by: List[dict]
    data: dict
    created_at: str
    updated_at: str
    is_labeled: bool
    overlap: int 
    project: int

class TaskList(BaseModel):
    total_annotations: int
    total: int
    tasks: List[Task]

    @property
    def n_incomplete(self):
        return self.total - self.total_annotations

class Project(BaseModel):
    id: int
    title: str
    description: Optional[str]
    label_config: str
    created_at: str
    created_by: dict
    num_tasks_with_annotations: Optional[int]
    task_number: Optional[int]

@dataclass
class APIProjects:

    def list_projects(self):
        content = requests.get(
            f"{self.url}/api/projects", 
            headers=self.headers
        )
        return [
            Project.parse_obj(p)
            for p in json.loads(content.content)["results"]
        ]

    def create_project(
        self, title: str = "new project", description: str = "description"
    ):

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

@dataclass
class APITasks:

    def get_tasks(self, project_id):
        tasklist = json.loads(
            requests.get(
                f"{self.url}/api/tasks", 
                headers=self.headers, 
                data={"project": project_id}
            ).content
        )
        tasklist["tasks"] = [Task.parse_obj(t) for t in tasklist["tasks"]]
        return TaskList.parse_obj(tasklist)

    def post_tasks(self, df, project_id):
        for _, row in df.iterrows():
            resp = requests.post(
                f"{self.url}/api/tasks", 
                headers=self.headers, 
                data={
                    "project": project_id, 
                    "data": json.dumps({"item": row.to_dict()})
                }
            )
            json.loads(resp.content)


@dataclass
class APIAnnotations:

    def _get_annotationlist_for_task(self, task_id):
        annotationlist = json.loads(
            requests.get(
                f"{self.url}/api/tasks/{task_id}/annotations/", 
                headers=self.headers
            ).content
        )
        if annotationlist:
            annotationlist = [
                Annotation.parse_obj(a)
                for a in annotationlist
            ]
        return annotationlist

    def _latest_annotation(self, annotationlist):
        return sorted(
            annotationlist, 
            key=lambda d: d.created_at
        )[0].label

    def get_new_labels(self, project_id):
        labels = {}
        for task in self.get_tasks(project_id=project_id).tasks:
            annotationlist = self._get_annotationlist_for_task(task_id=task.id)
            if annotationlist:
                labels[task.id] = {
                    **{"label":self._latest_annotation(annotationlist)},
                    **task.data["item"]
                }
        return pd.DataFrame(labels).T

@dataclass
class APIWebhooks:
    settings: Settings

    def get_webhooks(self):
        resp = requests.get(f"{self.url}/api/webhooks", headers=self.headers)
        return json.loads(resp.content)

    def post_webhook(self, project_id):
        query = {
            "project": project_id,
            "url": f"{self.settings.other.fast_api.url}/payload",
            "send_payload": False,
            "is_active": True,
            "actions": ["ANNOTATION_CREATED", "ANNOTATION_UPDATED"],
        }
        resp = requests.post(
            f"{self.url}/api/webhooks", headers=self.headers, data=query
        )


@dataclass
class LabelStudioAPI(APIProjects, APITasks, APIAnnotations, APIWebhooks):
    settings: Settings

    @property
    def url(self) -> str:
        return self.settings.other.label_studio.url

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"""Token {self.settings.other.label_studio.api_key}"""
        }