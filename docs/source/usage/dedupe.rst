Dedupe
----------------

Below is an example that dedupes `df` on attributes columns specified in settings.

train model
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   import glob
   import pandas as pd
   from oagdedupe.api import Dedupe

   files = glob.glob(
      "/mnt/Research.CF/References & Training/Satchel/dedupe_rl/baseline_datasets/north_carolina_voters/*"
   )[:2]
   df = pd.concat([pd.read_csv(f) for f in files]).reset_index(drop=True)
   for attr in settings.attributes:
      df[attr] = df[attr].astype(str)
   df = df.sample(100_000, random_state=1234)

   d = Dedupe(settings=settings)
   d.initialize(df=df, reset=True)

   # %%
   # pre-processes data and stores pre-processed data, comparisons, ID matrices in SQLite db
   d.fit_blocks()

start fastAPI
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Run 

.. code-block:: console

   DEDUPER_NAME="<project name>";
   DEDUPER_FOLDER="<project folder>";
   python -m dedupe.fastapi.main

replacing `<project name>` and `<project folder>` with your project settings (for the example above, `test` and `./.dedupe`).


label-studio
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Return to label-studio and start labelling. When the queue falls under 5 tasks, fastAPI will update the model with labelled samples then send more tasks to review.


predictions
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To get predictions, simply run the `predict()` method.

.. code-block:: python
   
   d = Dedupe(settings=Settings(name="test", folder="./.dedupe"))
   d.predict()

See :ref:`run.py<Dedupe Example>` for the full working example.
