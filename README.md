# dedupe  

## quickstart

#### 1. set up config file

There are two options:

(1) A global config file at ~/.dedupe/config.ini
(2) A per-project config file at $CWD/.dedupe/config.ini, where $CWD is the folder you're running your script from (the folder where you `import dedupe`).

The config file must be called `config.ini` contain:

```
[MAIN]
HOST = http://172.22.39.26
MODEL_FILEPATH = [path to model to load, or save if it does not exist]
CACHE_FILEPATH = [path to store database files for SQLite and comparison pairs]

[LABEL_STUDIO]
PORT = 8001
API_KEY = [API KEY from Label Studio interface after signing up / logging in]
TITLE = [Title of Dedupe Project]
DESCRIPTION = [Description of Project]

[FAST_API]
PORT = 8000
```

#### 2. train model

Below is an example that dedupes `df` on `business_addr_cols`.

It uses a manual blocking scheme to narrow possible comparisons.

```
from dedupe import block
from dedupe.api import Dedupe
from dedupe.distance.string import RayAllJaro

manual_blocker = block.ManualBlocker([
    [
        (block.FirstNLetters(N=1), "businesshousenumber"),
        (block.FirstNLetters(N=3), "businessstreetname"), 
        (block.FirstNLetters(N=2), "businesscity"), 
    ],
])

business_addr_cols = [
    'businesshousenumber', 'businessstreetname', 'businessapartment',
    'businesscity', 'businessstate', 'businesszip'
]

df = (
    hpd_regis_contacts[business_addr_cols]
    .drop_duplicates()
    .reset_index(drop=True)
)

d = Dedupe(
    df=df, 
    attributes=business_addr_cols, 
    blocker=manual_blocker,
    distance=RayAllJaro(), 
    cpus=15 # parallelize distance computations
)

# pre-processes data and stores pre-processed data, comparisons, ID matrices in SQLite db
d.train()
```

#### 3. start label-studio

1. Create `cache` folder at `cwd`
2. Start label-studio, e.g. on port 8089.
3. Once label-studio is running, log in (can make up any user/pw).
    - Go to "Account & Settings" using icon on top-right
    - Get Access Token and copy/paste into config file under `[LABEL_STUDIO]`, "API_KEY"

```
docker run -it -p 8089:8080 -v `pwd`/cache/mydata:/label-studio/data \
	--env LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true \
	--env LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/label-studio/files \
	-v `pwd`/cache/myfiles:/label-studio/files \
	heartexlabs/label-studio:latest label-studio
```

#### 4. start fastAPI

Run `python -m dedupe.fastapi.main.py`, making sure you are in same directory as the `.dedupe` folder

Then return to label-studio and start labelling. When the queue falls under 5 tasks, fastAPI will 
update the model with labelled samples then send more tasks to review.

#### 5. predictions

To get predictions, simply run the `predict()` method.

```
d = Dedupe()
d.predict()
```