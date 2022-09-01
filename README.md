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
```
See [dedupe/settings.py](./dedupe/settings.py) for the full settings code.

#### 3. train model

Below is an example that dedupes voter records on name and address columns.

It uses a manual blocking scheme to narrow possible comparisons.

```py
import glob
from dedupe.api import Dedupe
from dedupe.distance.string import RayAllJaro
from dedupe.block import blockers
from dedupe.block import algos

files = glob.glob(
    "/mnt/Research.CF/References & Training/Satchel/dedupe_rl/baseline_datasets/north_carolina_voters/*"
)[:1]


df = pd.concat([pd.read_csv(f) for f in files]).reset_index(drop=True).iloc[:10000]

for attr in attributes:
    df[attr] = df[attr].astype(str)


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

d = Dedupe(
    settings=settings, # defined above
    df=df,
    blocker=manual_blocker,
    distance=RayAllJaro(),
)

# %%
# pre-processes data and stores pre-processed data, comparisons, ID matrices in SQLite db
d.train()
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