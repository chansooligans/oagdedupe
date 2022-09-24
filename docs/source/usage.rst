Usage
=====

.. _installation:

Installation
------------

To use oagdedupe, first install it using pip:

.. note::

   Not yet available -- git clone and install

.. code-block:: console

   pip install oagdedupe

Set Up
----------------

1. start label-studio
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start label-studio, e.g. on port 8089.

.. code-block:: console

   docker run -it -p 8089:8080 -v `pwd`/cache/mydata:/label-studio/data \
      --env LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true \
      --env LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/label-studio/files \
      -v `pwd`/cache/myfiles:/label-studio/files \
      heartexlabs/label-studio:latest label-studio

2. postgres
^^^^^^^^^^^^^^^^^^^^^^^^^^^

[insert instructions here about initializing postgres]

most importantly, need to create functions (dedupe/postgres/funcs.py)


3. Define project settings
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Make a `dedupe.settings.Settings` object. For example:

.. code-block:: python

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
               "api_key": "[INSERT API KEY]",  # label studio port
               "description": "[project name]",  # label studio description of project
         },
         fast_api={"port": 8090},  # fast api port
      ),
   )
   settings.save()

To get label studio api_key:
   1. log in (can make up any user/pw).
   2. Go to "Account & Settings" using icon on top-right
   3. Get Access Token and copy/paste into settings at `settings.other.label_studio["api_key"]` 

See :ref:`dedupe.settings<Settings>` for the full settings code.


Dedupe
----------------

1. train model
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Below is an example that dedupes voter records on name and address columns.

It uses a manual blocking scheme to narrow possible comparisons.

.. code-block:: python

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
   d.initialize(df=df, reset=True)

   # %%
   # pre-processes data and stores pre-processed data, comparisons, ID matrices in SQLite db
   d.fit_blocks()

2. start fastAPI
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Run 

.. code-block:: console

   DEDUPER_NAME="<project name>";
   DEDUPER_FOLDER="<project folder>";
   python -m dedupe.fastapi.main

replacing `<project name>` and `<project folder>` with your project settings (for the example above, `test` and `./.dedupe`).


3. label-studio
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Return to label-studio and start labelling. When the queue falls under 5 tasks, fastAPI will update the model with labelled samples then send more tasks to review.


4. predictions
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To get predictions, simply run the `predict()` method.

.. code-block:: python
   
   d = Dedupe(settings=Settings(name="test", folder="./.dedupe"))
   d.predict()

See :ref:`run.py<Settings>` for the full working example.

