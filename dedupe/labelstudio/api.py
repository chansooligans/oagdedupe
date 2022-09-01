# %%
from dataclasses import dataclass
from typing import Dict
import requests
import json
import pandas as pd
from dedupe.settings import Settings, get_settings_from_env


@dataclass
class Projects:
    def list_projects(self):
        content = requests.get(f"{self.url}/api/projects", headers=self.headers)
        return json.loads(content.content)

    def get_project(self, project_id):
        resp = requests.get(
            f"{self.url}/api/projects/{project_id}", headers=self.headers
        )
        return json.loads(resp.content)

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
        return json.loads(resp.content)


@dataclass
class Tasks:
    def get_tasks(self, project_id):
        query = {"project": project_id}
        resp = requests.get(f"{self.url}/api/tasks", headers=self.headers, data=query)
        return json.loads(resp.content)

    def get_tasks_from_fastAPI(self):
        contents = requests.get(f"{config.fast_api_url}/samples")
        query_index = json.loads(contents.content)["query_index"]
        df = pd.DataFrame(json.loads(contents.content)["samples"]).drop("label", axis=1)
        df["idx"] = query_index
        return df

    def post_tasks(self, df, project_id):
        for _, row in df.iterrows():
            query = {"project": project_id, "data": json.dumps({"item": row.to_dict()})}

            resp = requests.post(
                f"{self.url}/api/tasks", headers=self.headers, data=query
            )
            json.loads(resp.content)


@dataclass
class Annotations:
    def get_annotation(self, task_id):
        return json.loads(
            requests.get(
                f"{self.url}/api/tasks/{task_id}/annotations/", headers=self.headers
            ).content
        )

    def get_all_annotations(self, project_id):
        task_ids = [x["id"] for x in self.get_tasks(project_id=project_id)["tasks"]]
        return {
            task_id: self.latest_annotation(self.get_annotation(task_id=task_id))
            for task_id in task_ids
            if self.get_annotation(task_id=task_id)
        }

    def latest_annotation(self, annotations):
        return sorted(annotations, key=lambda d: d["created_at"])[0]["result"][0][
            "value"
        ]["choices"][0]


@dataclass
class Webhooks:

    settings: Settings

    def get_webhooks(self):
        resp = requests.get(f"{self.url}/api/webhooks", headers=self.headers)
        return json.loads(resp.content)

    def post_webhook(self, project_id):
        query = {
            "project": project_id,
            "url": f"{self.settings.other.fast_api.url}/payload",
            "send_payload": True,
            "is_active": True,
            "actions": ["ANNOTATION_CREATED", "ANNOTATION_UPDATED"],
        }
        resp = requests.post(
            f"{self.url}/api/webhooks", headers=self.headers, data=query
        )


@dataclass
class LabelStudioAPI(Projects, Tasks, Annotations, Webhooks):

    settings: Settings = get_settings_from_env()


    @property
    def url(self) -> str:
        assert self.settings.other is not None
        return self.settings.other.label_studio.url

    @property
    def headers(self) -> Dict[str, str]:
        assert self.settings.other is not None
        return {
            "Authorization": f"""Token {self.settings.other.label_studio.api_key}"""
        }

