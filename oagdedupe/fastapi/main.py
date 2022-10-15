"""this script is used to boot up fastapi
"""

import argparse
import json
import logging
import time
from typing import Union

import joblib
import numpy as np
import pandas as pd
import uvicorn
from sqlalchemy import types
from tqdm import tqdm

from oagdedupe._typing import Dists
from oagdedupe.fastapi import app, fapi
from oagdedupe.labelstudio import lsapi
from oagdedupe.settings import Settings

root = logging.getLogger()
root.setLevel(logging.DEBUG)


parser = argparse.ArgumentParser()
parser.add_argument("--settings", help="set settings file location")
args = parser.parse_args()

if args.settings:
    settings = Settings(args.settings)
else:
    settings = Settings()

while fapi.url_checker(settings.label_studio.url) == False:
    logging.info("waiting for label studio...")
    time.sleep(3)

m = fapi.Model(settings=settings)
m.initialize_learner()


@app.on_event("startup")
async def startup():
    """
    on startup:
    (1) create project if not exists
    (2) generate new samples if needed
    """
    m.initialize_project()
    m.generate_new_samples()


@app.post("/train")
async def train() -> None:
    """
    update model then make predictions on full data;
    load predictions to "scores" table
    """
    m._train()
    logging.info(f"save model to {settings.model.path_model}")
    joblib.dump(m.clf.estimator, settings.model.path_model)


@app.post("/predict")
async def predict(dists: Dists) -> Dists:
    """
    update model then make predictions on full data;
    load predictions to "scores" table
    """
    dists = np.array(dists.dists)
    res = m.clf.predict_proba(dists[:, :-2])
    return res.tolist()


@app.post("/payload")
async def payload() -> None:
    m.generate_new_samples()


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=settings.fast_api.host.split("/")[-1],
        port=settings.fast_api.port,
    )
