from dedupe.fastapi import utils as u
from dedupe.fastapi import app
from dedupe.settings import get_settings_from_env
from typing import Union

import time
import joblib
import uvicorn
import logging

root = logging.getLogger()
root.setLevel(logging.DEBUG)


settings = get_settings_from_env()
assert settings.other is not None
while u.url_checker(settings.other.label_studio.url) == False:
    logging.info("waiting for label studio...")
    time.sleep(3)

m = u.Model(settings=settings)


@app.on_event("startup")
async def startup():
    m.generate_new_samples()


@app.get("/predict")
async def predict():

    logging.info(f"save model to {settings.other.path_model}")
    joblib.dump(m.clf.estimator, settings.other.path_model)

    return dict(
        {
            "predict_proba": m.clf.predict_proba(m.X).reshape(1, -1).tolist()[0],
            "predict": m.clf.predict(m.X).tolist(),
        }
    )


@app.post("/payload")
async def payload(data: Union[u.Annotation, None]):
    m.generate_new_samples()


if __name__ == "__main__":
    uvicorn.run(
        app, host=settings.other.fast_api.host.split("/")[-1], port=settings.other.fast_api.port
    )
