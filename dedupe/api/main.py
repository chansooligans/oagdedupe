# %%
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

from modAL.models import ActiveLearner
from modAL.uncertainty import uncertainty_sampling
import joblib 

import numpy as np
import pandas as pd

import json
from json import JSONEncoder
import uvicorn
active_model_fp = "/mnt/Research.CF/References & Training/Satchel/dedupe_rl/active_models"

# %% [markdown]
"""
# Initialize
"""

# %%
import sqlite3
from sqlalchemy import create_engine
cache_fp="../../cache/test.db"
engine = create_engine(f"sqlite:///{cache_fp}", echo=False)

X = pd.read_sql_query("select * from distances", con=engine)

attributes = ["givenname","surname","suburb","postcode"]

clf = ActiveLearner(
    estimator=joblib.load(f"{active_model_fp}/nc_benchmark_10k.pkl"),
    query_strategy=uncertainty_sampling
)

clf.teach(np.repeat(1, len(attributes)).reshape(1, -1), [1])

# %% [markdown]
"""
# API
"""

# %%
class Query(BaseModel):
    query_index: List[int]
    samples: dict

app = FastAPI()

@app.get("/samples")
async def get_samples(n_instances=5, response_model=Query):

    ignore_idx = list(pd.read_sql("SELECT distinct idx FROM labels", con=engine)["idx"].values)
    X_subset = X.drop(ignore_idx, axis=0)
    
    subset_idx, _ = clf.query(
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
            """, con=engine
        ).drop("idx",axis=1)

    samples.columns = [x+"_l" for x in attributes] + [x+"_r" for x in attributes]
    samples["label"] = None

    return dict({
        "query_index": query_index.tolist(),
        "samples":samples.to_dict()
    })

@app.post("/submit")
async def submit_labels(labels: Query):
    samples = pd.DataFrame(labels.samples)
    samples["idx"] = labels.query_index
    samples.to_sql("labels", con=engine, if_exists="append", index=False)

@app.post("/train")
async def train():
    
    df = pd.read_sql("""
        SELECT idx,label FROM labels
        WHERE label in (0, 1)
    """, con=engine).drop_duplicates()

    clf.teach(
        X = X.loc[list(df["idx"])],
        y = df["label"]
    )

@app.post("/get_labels")
async def get_labels():
    
    df = pd.read_sql("""
        SELECT * FROM labels
    """, con=engine).drop_duplicates()

    return df.to_dict()


if __name__=="__main__":
    uvicorn.run(app,host="127.0.0.1",port=8000)
    
# %%

# %%
