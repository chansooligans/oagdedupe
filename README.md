# dedupe  

#### 1. start label-studio

1. Start label-studio, e.g. on port 8089.
2. Once label-studio is running, log in (can make up any user/pw).
    - Go to "Account & Settings" using icon on top-right
    - Get Access Token and copy/paste into config file under `[LABEL_STUDIO]`, "API_KEY"

```
docker run -it -p 8089:8080 -v `pwd`/cache/mydata:/label-studio/data \
	--env LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true \
	--env LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/label-studio/files \
	-v `pwd`/cache/myfiles:/label-studio/files \
	heartexlabs/label-studio:latest label-studio
```


#### 2. Define project settings

Make a `dedupe.settings.Settings` object. For example:
```py
from dedupe.settings import (
    Settings,
    SettingsOther,
)

settings = Settings(
    name="default",  # the name of the project, a unique identifier
    folder="./.dedupe",  # path to folder where settings and data will be saved
    other=SettingsOther(
        n=5000, # active-learning samples per learning loop
        k=3, # max_len of block conjunctions
        cpus=20,  # parallelize distance computations
        attributes=["givenname", "surname", "suburb", "postcode"],  # list of entity attribute names
        path_database="postgresql+psycopg2://username:password@172.22.39.26:8000/db",  # where to save the sqlite database holding intermediate data
        db_schema="dedupe",
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
```
See [dedupe/settings.py](./dedupe/settings.py) for the full settings code.

#### 3. train model

Below is an example that dedupes voter records on name and address columns.

It uses a manual blocking scheme to narrow possible comparisons.

```py
import glob
import pandas as pd
from dedupe.api import Dedupe

files = glob.glob(
    "/mnt/Research.CF/References & Training/Satchel/dedupe_rl/baseline_datasets/north_carolina_voters/*"
)[:2]
df = pd.concat([pd.read_csv(f) for f in files]).reset_index(drop=True)
for attr in settings.other.attributes:
    df[attr] = df[attr].astype(str)
df = df.sample(100_000, random_state=1234)

d = Dedupe(settings=settings)
d.initialize(df=df)

# %%
# pre-processes data and stores pre-processed data, comparisons, ID matrices in SQLite db
d.fit_blocks()
```

#### 4. start fastAPI

Run 

```sh
DEDUPER_NAME="<project name>" DEDUPER_FOLDER="<project folder>"  python -m dedupe.fastapi.main
```

replacing `<project name>` and `<project folder>` with your project settings (for the example above, `test` and `./.dedupe`).

Then return to label-studio and start labelling. When the queue falls under 5 tasks, fastAPI will update the model with labelled samples then send more tasks to review.

#### 5. predictions

To get predictions, simply run the `predict()` method.

```py
d = Dedupe(settings=Settings(name="test", folder="./.dedupe"))
d.predict()
```

See [./run.py](./run.py) for the full working example.