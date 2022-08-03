# %%
from dataclasses import dataclass
import requests
import json
import pandas as pd
from dedupe.config import Config
config = Config()

@dataclass
class Projects:

    def list_projects(self):
        content = requests.get(f"{self.url}/api/projects", headers=self.headers)
        return json.loads(content.content)
        
    def get_project(self, project_id):
        resp = requests.get(f"{self.url}/api/projects/{project_id}", headers=self.headers)
        return json.loads(resp.content)

    def create_project(self, title:str = "new project", description:str ="description"):

        new_project = {
            "title":title,
            "description":description,
            "label_config":"""
                <View>
                    <Header value="Table with {key: value} pairs"/>
                    <Table name="table" value="$item"/>
                    <Choices name="choice" toName="table">
                        <Choice value="Match"/>
                        <Choice value="Not a Match"/>
                        <Choice value="Uncertain"/>
                    </Choices>
                </View>
            """
        }
        
        requests.post(f"{self.url}/api/projects", headers=self.headers, data=new_project)

@dataclass
class Tasks:
    
    def get_tasks(self, project_id):
        query = {
            "project":project_id
        }
        resp = requests.get(f"{self.url}/api/tasks", headers=self.headers, data=query)
        return json.loads(resp.content)

    def get_tasks_from_fastAPI(self):
        contents = requests.get(f"{config.fast_api_url}/samples")
        query_index = json.loads(contents.content)["query_index"]
        df = pd.DataFrame(json.loads(contents.content)["samples"]).drop("label", axis=1)
        df["idx"] = query_index
        return df

    def post_tasks(self, df):
        for _,row in df.iterrows():
            query = {
                "project":10,
                "data":json.dumps({"item":row.to_dict()})
            }

            resp = requests.post(f"{self.url}/api/tasks", headers=self.headers, data=query)
            json.loads(resp.content)

@dataclass
class Annotations:

    def get_annotations(self):
        return

@dataclass
class Webhooks:

    def get_webhooks(self):
        resp = requests.get(f"{self.url}/api/webhooks", headers=self.headers)
        return json.loads(resp.content)

    def post_webhook(self, project_id):
        query = {
            "project":project_id,
            "url":"http://172.22.39.26:8000/payload",
            "send_payload":True,
            "is_active":True,
            "actions":["ANNOTATION_CREATED", "ANNOTATION_UPDATED"]
        }
        resp = requests.post(f"{self.url}/api/webhooks", headers=self.headers, data=query)


@dataclass
class LabelStudioAPI(
    Projects,
    Tasks,
    Annotations,
    Webhooks
):
    url = config.ls_url
    headers = {"Authorization":f"""Token {config.ls_api_key}"""}

