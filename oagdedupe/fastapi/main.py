from oagdedupe.fastapi import fapi
from oagdedupe.fastapi import app
from oagdedupe.labelstudio import lsapi
from oagdedupe.settings import get_settings_from_env
from typing import Union
from sqlalchemy import select

import numpy as np
import pandas as pd
from tqdm import tqdm
import time
import joblib
import uvicorn
import logging

root = logging.getLogger()
root.setLevel(logging.DEBUG)

settings = get_settings_from_env()
assert settings.other is not None
while fapi.url_checker(settings.other.label_studio.url) == False:
    logging.info("waiting for label studio...")
    time.sleep(3)

m = fapi.Model(settings=settings)
m.initialize_learner()


@app.on_event("startup")
async def startup():
    m.initialize_project()
    m.generate_new_samples()


@app.post("/predict")
async def predict():

    m._train()

    logging.info(f"save model to {settings.other.path_model}")
    joblib.dump(m.clf.estimator, settings.other.path_model)

    m.api.init.engine.execute(
        f"TRUNCATE TABLE {settings.other.db_schema}.scores"
    )

    session = m.api.orm.Session()
    stmt = select(
        *(
            getattr(m.api.init.FullDistances,x)
            for x in settings.other.attributes + ["_index_l", "_index_r"]
        )
    ).execution_options(yield_per=50000)

    for partition in tqdm(session.execute(stmt).partitions()):
        
        dists = np.array([
            [getattr(row, x) for x in settings.other.attributes + ["_index_l", "_index_r"]]
            for row in partition]
        )
        
        probs = pd.DataFrame(
            np.hstack([m.clf.predict_proba(dists[:, :-2])[:,1:],dists[:,-2:]]),
            columns = ["score","_index_l", "_index_r"]
        )

        probs.to_sql(
            "scores",
            schema=settings.other.db_schema,
            if_exists="append",
            con=m.api.init.engine,
            index=False
        )


@app.post("/payload")
async def payload():
    m.generate_new_samples()


if __name__ == "__main__":
    uvicorn.run(
        app, host=settings.other.fast_api.host.split("/")[-1], port=settings.other.fast_api.port
    )
