# oagdedupe  

oagdedupe is a Python library for scalable entity resolution, using active 
learning to learn blocking configurations, generate comparison pairs, 
then clasify matches. 

## page contents
- [Documentation](#documentation)
- [Installation](#installation)
    - [label-studio](#label-studio)
    - [postgres](#postgres)
    - [project settings](#project-settings)
- [dedupe](#dedupe-example)
- [record-linkage](#record-linkage-example)
    
# Documentation<a name="#documentation"></a>

You can find the documentation of oagdedupe at https://deduper.readthedocs.io/en/latest/, 
where you can find the [api reference](https://deduper.readthedocs.io/en/latest/dedupe/api.html), 
[guide to methodology](https://deduper.readthedocs.io/en/latest/userguide/intro.html),
and [examples](https://deduper.readthedocs.io/en/latest/examples/example_dedupe.html).

# Installation<a name="#installation"></a>

[tbd pip install instructions]

## start label-studio<a name="#label-studio"></a>

Start label-studio using docker command below, updating `[LS_PORT]` to the 
port on your host machine

```
docker run -it -p [LS_PORT]:8080 -v `pwd`/cache/mydata:/label-studio/data \
	--env LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true \
	--env LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/label-studio/files \
	-v `pwd`/cache/myfiles:/label-studio/files \
	heartexlabs/label-studio:latest label-studio
```

## postgres<a name="#postgres"></a>

[insert instructions here about initializing postgres]

most importantly, need to create functions (dedupe/postgres/funcs.py)

## project settings<a name="#project-settings"></a>

Make a `dedupe.settings.Settings` object. For example:
```py
from oagdedupe.settings import (
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
To get label studio api_key:
   1. log in (can make up any user/pw).
   2. Go to "Account & Settings" using icon on top-right
   3. Get Access Token and copy/paste into settings at `settings.other.label_studio["api_key"]` 

See [dedupe/settings.py](./dedupe/settings.py) for the full settings code.

# dedupe<a name="#dedupe-example"></a>

Below is an example that dedupes `df` on attributes columns specified in settings.

## train dedupe<a name="#train-dedupe"></a>

```py
import glob
import pandas as pd
from oagdedupe.api import Dedupe

d = Dedupe(settings=settings)
d.initialize(df=df, reset=True)

# %%
# pre-processes data and stores pre-processed data, comparisons, ID matrices in SQLite db
d.fit_blocks()
```

# record-linkage<a name="#record-linkage-example"></a>

Below is an example that links `df` to `df2`, on attributes columns specified 
in settings (dataframes should share these columns).

## train record-linkage<a name="#train-record-linkage"></a>
```py
import glob
import pandas as pd
from oagdedupe.api import RecordLinkage

d = RecordLinkage(settings=settings)
d.initialize(df=df, df2=df2, reset=True)

# %%
# pre-processes data and stores pre-processed data, comparisons, ID matrices in SQLite db
d.fit_blocks()
```

# active learn<a name="#active-learn"></a>

For either dedupe or record-linkage, run:

```sh
   DEDUPER_NAME="<project name>";
   DEDUPER_FOLDER="<project folder>";
   python -m dedupe.fastapi.main
```

replacing `<project name>` and `<project folder>` with your project settings (for the example above, `test` and `./.dedupe`).

Then return to label-studio and start labelling. When the queue falls under 5 tasks, fastAPI will update the model with labelled samples then send more tasks to review.

# predictions<a name="#predictions"></a>

To get predictions, simply run the `predict()` method.

Dedupe:
```py
d = Dedupe(settings=Settings(name="test", folder="./.dedupe"))
d.predict()
```

Record-linkage:
```py
d = RecordLinkage(settings=Settings(name="test", folder="./.dedupe"))
d.predict()
```