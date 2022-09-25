from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

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
