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
from dedupe.distance.string import RayAllJaro
from dedupe.block import blockers
from dedupe.block import algos

# %%
attributes = ["givenname", "surname", "suburb", "postcode"]


settings = Settings(
    name="test",  # the name of the project, a unique identifier
    folder="./.dedupe",  # path to folder where settings and data will be saved
    other=SettingsOther(
        cpus=15,  # parallelize distance computations
        attributes=attributes,  # list of entity attribute names
        path_database="./.dedupe/test.db",  # where to save the sqlite database holding intermediate data
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
for attr in attributes:
    df[attr] = df[attr].astype(str)


# %%
manual_blocker = blockers.ManualBlocker(
    [
        [
            (algos.FirstNLetters(N=2), "givenname"),
            (algos.FirstNLetters(N=2), "surname"),
            (algos.ExactMatch(), "suburb"),
        ],
        [
            (algos.FirstNLetters(N=2), "givenname"),
            (algos.FirstNLetters(N=2), "surname"),
            (algos.ExactMatch(), "postcode"),
        ],
    ]
)

# %%
d = Dedupe(
    settings=settings,  # defined above
    df=df,
    blocker=manual_blocker,
    distance=RayAllJaro(),
)

# %%
# pre-processes data and stores pre-processed data, comparisons, ID matrices in SQLite db
d.train()

# %%
# now
# 1. start up fast api, and
# 2. label on label studio

# %%
d = Dedupe(settings=Settings(name="test", folder="./.dedupe"))
d.predict()


# %%
preds = d.predict()
df.merge(preds, left_index=True, right_on="id").sort_values("cluster").head(20)

# %%
