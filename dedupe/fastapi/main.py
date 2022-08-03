from dedupe.fastapi import utils as u
from dedupe.fastapi import app
from dedupe import config

import pandas as pd
import joblib 
import uvicorn
import logging
root = logging.getLogger()
root.setLevel(logging.DEBUG)

# api
m = u.Model(cache_fp=config.cache_fp, active_model_fp=config.model_fp)

@app.on_event("startup")
async def startup():
    """
    On Startup
    Check for project. If project does not exist, create project.
    Check for tasks. If tasks do not exist, submit tasks.
    """
    m.generate_new_samples()

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
    m.generate_new_samples()

if __name__=="__main__":
    uvicorn.run(
        app,host=config.host.split("/")[-1],
        port=int(config.fast_api_port)
    )
    
@app.get("/samples")
async def get_samples(n_instances:int=10):
    """
    not used in active learning loop
    """

    query_index, samples = m.get_samples(n_instances=n_instances)
    return dict({
        "query_index": query_index.tolist(),
        "samples":samples.to_dict()
    })

@app.post("/submit")
async def submit_labels(labels: u.Query):
    """
    not used in active learning loop
    """

    samples = pd.DataFrame(labels.samples)
    samples["idx"] = labels.query_index
    samples.to_sql("labels", con=m.engine, if_exists="append", index=False)

@app.get("/get_labels")
async def get_labels():
    """
    not used in active learning loop
    """

    df = pd.read_sql("""
        SELECT * FROM labels
    """, con=m.engine).drop_duplicates()

    return df.to_dict()