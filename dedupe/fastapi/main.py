from dedupe.fastapi import utils as u
from dedupe.fastapi import app
from dedupe import config
from typing import Union

import time
import pandas as pd
import joblib 
import uvicorn
import logging
root = logging.getLogger()
root.setLevel(logging.DEBUG)

while u.url_checker(config.ls_url) == False:
    logging.info("waiting for label studio...")
    time.sleep(3)

m = u.Model()

@app.on_event("startup")
async def startup():
    m.generate_new_samples()

@app.get("/predict")
async def predict():

    logging.info(f"save model to {config.model_fp}")
    joblib.dump(m.clf.estimator, config.model_fp)
    
    return dict({
        "predict_proba":m.clf.predict_proba(m.X).reshape(1,-1).tolist()[0],
        "predict":m.clf.predict(m.X).tolist()
    })

@app.post("/payload")
async def payload(data:Union[u.Annotation, None]):
    m.generate_new_samples()

if __name__=="__main__":
    uvicorn.run(
        app,host=config.host.split("/")[-1],
        port=int(config.fast_api_port)
    )
    