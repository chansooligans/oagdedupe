# %%
from IPython import get_ipython

if get_ipython() is not None:
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")

# %%
import glob
import pandas as pd
from dedupe.settings import (
    Settings,
    SettingsOther,
)
from dedupe.api import Dedupe
from sqlalchemy import create_engine
engine = create_engine("postgresql+psycopg2://username:password@172.22.39.26:8000/db")

# %%
settings = Settings(
    name="default",  # the name of the project, a unique identifier
    folder="./.dedupe",  # path to folder where settings and data will be saved
    other=SettingsOther(
        mem=False,
        n=5000,
        k=3,
        cpus=15,  # parallelize distance computations
        attributes=["givenname", "surname", "suburb", "postcode"],  # list of entity attribute names
        path_database="postgresql+psycopg2://username:password@172.22.39.26:8000/db",  # where to save the sqlite database holding intermediate data
        schema="dedupe",
        path_model="./.dedupe/test_model",  # where to save the model
        label_studio={
            "port": 8089,  # label studio port
            "api_key": "83e2bc3da92741aa41c272829558c596faefa745",  # label studio port
            "description": "chansoo test project",  # label studio description of project
        },
        fast_api={"port": 8090},  # fast api port
    ),
)
settings.save()

# %%
files = glob.glob(
    "/mnt/Research.CF/References & Training/Satchel/dedupe_rl/baseline_datasets/north_carolina_voters/*"
)[:2]
df = pd.concat([pd.read_csv(f) for f in files]).reset_index(drop=True)
for attr in settings.other.attributes:
    df[attr] = df[attr].astype(str)

# %%
%%time
d = Dedupe(settings=settings)
d.initialize(df=None)

# %% [markdown]
"""
################################################################
################################################################
################################################################

testing to get m.distances on FULL data based on best conjunctions:

################################################################
################################################################
################################################################
"""

# %%
%%time
d.fit()

# %%
pd.read_sql("select * from dedupe.full_distances limit 100", con=engine)
