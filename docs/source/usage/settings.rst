Settings
----------------

Make a `dedupe.settings.Settings` object. For example:

.. code-block:: python

   from oagdedupe.settings import (
      Settings,
      SettingsModel,
      SettingsDB,
      SettingsLabelStudio,
      SettingsService
   )

   settings = Settings(
        name="default",  # the name of the project, a unique identifier
        folder=".././.dedupe",  # path to folder where settings and data will be saved
        attributes=["givenname", "surname", "suburb", "postcode"],  # list of entity attribute names
        model=SettingsModel(
            dedupe=True,
            n=1000,
            k=3,
            max_compare=20_000,
            n_covered=5_000,
            cpus=20,  # parallelize distance computations
            path_model="./.dedupe/test_model",  # where to save the model
        ),
        db=SettingsDB(
            path_database="postgresql+psycopg2://username:password@172.22.39.26:8000/db",  # where to save the sqlite database holding intermediate data
            db_schema="dedupe",
        ),
        label_studio=SettingsLabelStudio(
            api_key="33344e8a477f8adc3eb6aa1e41444bde76285d96",  # label studio port
            description="chansoo test project",  # label studio description of project
            port=8089,  # label studio port
        ),
        fast_api = SettingsService(
            port=8090,  # fast api port
        )
    )

To get label studio api_key:
   1. log in (can make up any user/pw).
   2. Go to "Account & Settings" using icon on top-right
   3. Get Access Token and copy/paste into settings at `settings.label_studio["api_key"]` 

See :ref:`dedupe.settings<Settings>` for the full settings code.
