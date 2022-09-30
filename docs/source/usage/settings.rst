Settings
----------------

Make a `dedupe.settings.Settings` object. For example:

.. code-block:: python

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
         path_database="postgresql+psycopg2://username:password@0.0.0.0:8000/db",  # where to save the sqlite database holding intermediate data
         db_schema="dedupe",
         path_model="./.dedupe/test_model",  # where to save the model
         label_studio={
               "port": 8089,  # label studio port
               "api_key": "[INSERT API KEY]",  # label studio port
               "description": "[your project name]",  # label studio description of project
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
