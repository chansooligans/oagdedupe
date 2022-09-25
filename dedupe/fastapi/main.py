from oagdedupe.fastapi import fapi
from oagdedupe.fastapi import app
from oagdedupe.labelstudio import lsapi
from oagdedupe.settings import get_settings_from_env
from typing import Union

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


@app.get("/predict")
async def predict():

    logging.info(f"save model to {settings.other.path_model}")
    joblib.dump(m.clf.estimator, settings.other.path_model)

    return dict(
        {
            "predict_proba": m.clf.predict_proba(
                m.api.orm.get_full_distances()[settings.other.attributes].values
            )[:,1].tolist(),
            "predict": m.clf.predict(
                m.api.orm.get_full_distances()[settings.other.attributes].values
            ).tolist(),
        }
    )


@app.post("/payload")
async def payload():
    m.generate_new_samples()


if __name__ == "__main__":
    uvicorn.run(
        app, host=settings.other.fast_api.host.split("/")[-1], port=settings.other.fast_api.port
    )
