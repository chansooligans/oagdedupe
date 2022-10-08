Dedupe Example
=====

.. code-block:: python

    from oagdedupe.settings import (
        Settings, 
        SettingsModel,
        SettingsDB,
        SettingsLabelStudio,
        SettingsService
    )
    from oagdedupe.api import Dedupe

    import glob
    import pandas as pd
    pd.options.display.precision = 12
    from sqlalchemy import create_engine
    engine = create_engine("postgresql+psycopg2://username:password@0.0.0.0:8000/db")

    # %%
    settings = settings = Settings(
        attributes=["givenname", "surname", "suburb", "postcode"]
    )

    # %%
    files = glob.glob(
        "/mnt/Research.CF/References & Training/Satchel/dedupe_rl/baseline_datasets/north_carolina_voters/*"
    )[:2]
    df = pd.concat([pd.read_csv(f) for f in files]).reset_index(drop=True)
    for attr in settings.attributes:
        df[attr] = df[attr].astype(str)

    # %%
    d = Dedupe(settings=settings)
    d.initialize(df=df, reset=True)

    # %%
    d.fit_blocks()
    res = d.predict()