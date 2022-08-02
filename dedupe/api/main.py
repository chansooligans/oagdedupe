from dedupe.api import utils as u

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel
from typing import List

import joblib 
import pandas as pd
import uvicorn
import argparse
import logging
root = logging.getLogger()
root.setLevel(logging.DEBUG)

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

m = u.Model()
app = FastAPI()

def custom_openapi():

    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="Dedupe",
        version="2.0.0",
        routes=app.routes,
    )

    app.openapi_schema = openapi_schema

    return app.openapi_schema

app.openapi = custom_openapi

@app.get("/samples")
async def get_samples(n_instances:int=5):

    ignore_idx = list(pd.read_sql("SELECT distinct idx FROM labels", con=m.engine)["idx"].values)
    X_subset = m.X.drop(ignore_idx, axis=0)
    
    subset_idx, _ = m.clf.query(
        X_subset, 
        n_instances=n_instances
    )

    query_index = X_subset.index[subset_idx]

    samples = pd.read_sql_query(f"""
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
            """, con=m.engine
        ).drop("idx",axis=1)

    samples.columns = [x+"_l" for x in m.attributes] + [x+"_r" for x in m.attributes]
    samples["label"] = None

    return dict({
        "query_index": query_index.tolist(),
        "samples":samples.to_dict()
    })

@app.post("/submit")
async def submit_labels(labels: u.Query):
    samples = pd.DataFrame(labels.samples)
    samples["idx"] = labels.query_index
    samples.to_sql("labels", con=m.engine, if_exists="append", index=False)

@app.post("/train")
async def train():
    
    df = pd.read_sql("""
        SELECT idx,label FROM labels
        WHERE label in (0, 1)
    """, con=m.engine).drop_duplicates()

    m.clf.teach(
        X = m.X.loc[list(df["idx"])],
        y = df["label"]
    )

@app.post("/get_labels")
async def get_labels():
    
    df = pd.read_sql("""
        SELECT * FROM labels
    """, con=m.engine).drop_duplicates()

    return df.to_dict()

@app.post("/predict")
async def predict():

    logging.info(f"save model to {m.active_model_fp}")
    joblib.dump(m.clf.estimator, m.active_model_fp)
    
    return dict({
        "predict_proba":m.clf.predict_proba(m.X).reshape(1,-1).tolist()[0],
        "predict":m.clf.predict(m.X).tolist()
    })

if __name__=="__main__":
    uvicorn.run(app,host="172.22.39.26",port=8000)
    