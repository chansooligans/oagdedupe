"""this script is used to boot up fastapi
"""

import argparse
import logging
import time
from typing import Union

import joblib
import numpy as np
import pandas as pd
import uvicorn
from sqlalchemy import select
from tqdm import tqdm

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


@app.post("/predict")
async def predict() -> None:
    """
    update model then make predictions on full data;
    load predictions to "scores" table
    """

    m._train()

    logging.info(f"save model to {settings.model.path_model}")
    joblib.dump(m.clf.estimator, settings.model.path_model)

    m.api.init.engine.execute(f"TRUNCATE TABLE {settings.db.db_schema}.scores")

    with m.api.orm.Session() as session:

        stmt = m.api.orm.full_distance_partitions()

        for partition in tqdm(session.execute(stmt).partitions()):

            dists = np.array(
                [
                    [
                        getattr(row, x)
                        for x in settings.attributes + ["_index_l", "_index_r"]
                    ]
                    for row in partition
                ]
            )

            probs = pd.DataFrame(
                np.hstack(
                    [m.clf.predict_proba(dists[:, :-2])[:, 1:], dists[:, -2:]]
                ),
                columns=["score", "_index_l", "_index_r"],
            )

            probs.to_sql(
                "scores",
                schema=settings.db.db_schema,
                if_exists="append",
                con=m.api.init.engine,
                index=False,
            )


@app.post("/payload")
async def payload() -> None:
    m.generate_new_samples()


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=settings.fast_api.host.split("/")[-1],
        port=settings.fast_api.port,
    )
