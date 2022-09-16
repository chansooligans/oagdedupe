# %%
from IPython import get_ipython

if get_ipython() is not None:
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")

# %%
import glob
import pandas as pd
import numpy as np
from dedupe.settings import (
    Settings,
    SettingsOther,
)
from dedupe.api import Dedupe
from dedupe.distance.string import RayAllJaro

# %%
attributes = ["givenname", "surname", "suburb", "postcode"]


settings = Settings(
    name="test",  # the name of the project, a unique identifier
    folder="./.dedupe",  # path to folder where settings and data will be saved
    other=SettingsOther(
        mem=False,
        n=500,
        k=3,
        cpus=15,  # parallelize distance computations
        attributes=attributes,  # list of entity attribute names
        path_database="postgresql+psycopg2://username:password@172.22.39.26:8000/db",  # where to save the sqlite database holding intermediate data
        schema="dedupe",
        path_model="./.dedupe/test_model",  # where to save the model
        label_studio={
            "port": 8089,  # label studio port
            "api_key": "bc66ff77abeefc91a5fecd031fc0c238f9ad4814",  # label studio port
            "description": "gs test project",  # label studio description of project
        },
        fast_api={"port": 8003},  # fast api port
    ),
)

# %%
files = glob.glob(
    "/mnt/Research.CF/References & Training/Satchel/dedupe_rl/baseline_datasets/north_carolina_voters/*"
)[:1]


# %%
df = pd.concat([pd.read_csv(f) for f in files]).reset_index(drop=True).iloc[:10000]


# %%
for attr in settings.other.attributes:
    df[attr] = df[attr].astype(str)


# %%
%%time
from dedupe.block import Blocker, Coverage
blocker = Blocker(settings=settings)
blocker.initialize(df=df, attributes=attributes)
cover = Coverage(settings=settings)
cover.save()

# %%
d = Dedupe(
    settings=settings,  # defined above
    df=df,
    distance=RayAllJaro(),
)

# %%
d.train()

# %%
from sqlalchemy import create_engine
engine = create_engine("postgresql+psycopg2://username:password@172.22.39.26:8000/db")
pd.read_sql("""
    SELECT *
    FROM dedupe.distances
""", con=engine)

# %%
from dedupe.fastapi import utils as u
m = u.Model(settings=settings)

# %%
settings.path
# %%
