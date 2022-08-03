from dedupe.fastapi import utils as u
from dedupe.fastapi import app
from fastapi.responses import RedirectResponse

import json
import requests
import joblib 
import pandas as pd
import uvicorn
import argparse
import logging
root = logging.getLogger()
root.setLevel(logging.DEBUG)

from dedupe.config import Config
config = Config()

from dedupe.labelstudio.api import LabelStudioAPI
lsapi = LabelStudioAPI()

# e.g.
# python main.py --model /mnt/Research.CF/References\ \&\ Training/Satchel/dedupe_rl/active_models/nc_benchmark_10k.pkl --cache ../../cache/test.db
parser = argparse.ArgumentParser(description="""Fast API for a dedupe active learning model""")
parser.add_argument(
    '--model',
    help='optional model file path to pre-load a model'
)
parser.add_argument(
    '--cache',
    help='optional cache file path to pre-load a model; else creates cache folder in main repository'
)
args = parser.parse_args()

m = u.Model(cache_fp=args.cache, active_model_fp=args.model)

def check_queue_and_add():
    for proj in lsapi.list_projects()["results"]:
        if proj["title"] == "new project":
            break
    
    tasks = lsapi.get_tasks(project_id=proj["id"])
    n_incomplete = tasks["total"] - tasks["total_annotations"]
    if  n_incomplete < 10:
        query_index, df = m.get_samples(n_instances=10-n_incomplete)
        df["idx"] = query_index
        lsapi.post_tasks(df=df)

@app.on_event("startup")
async def startup():
    """
    Check for project. If project does not exist, create project.
    Check for tasks. If tasks do not exist, submit tasks.
    """
    check_queue_and_add()

@app.get("/samples")
async def get_samples(n_instances:int=10):
    query_index, samples = m.get_samples(n_instances=n_instances)
    return dict({
        "query_index": query_index.tolist(),
        "samples":samples.to_dict()
    })

@app.post("/submit")
async def submit_labels(labels: u.Query):
    samples = pd.DataFrame(labels.samples)
    samples["idx"] = labels.query_index
    samples.to_sql("labels", con=m.engine, if_exists="append", index=False)

@app.get("/get_labels")
async def get_labels():
    
    df = pd.read_sql("""
        SELECT * FROM labels
    """, con=m.engine).drop_duplicates()

    return df.to_dict()

@app.get("/predict")
async def predict():

    logging.info(f"save model to {m.active_model_fp}")
    joblib.dump(m.clf.estimator, m.active_model_fp)
    
    return dict({
        "predict_proba":m.clf.predict_proba(m.X).reshape(1,-1).tolist()[0],
        "predict":m.clf.predict(m.X).tolist()
    })


@app.post("/payload")
async def payload(data:u.Annotation):
    print(data.annotation)

if __name__=="__main__":
    uvicorn.run(
        app,host=config.host.split("/")[-1],
        port=int(config.fast_api_port)
    )
