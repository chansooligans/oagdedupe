Record Linkage Example
=====

.. code-block:: python

    from oagdedupe.settings import (
        Settings, 
        SettingsModel,
        SettingsDB,
        SettingsLabelStudio,
        SettingsService
    )
    from oagdedupe.api import RecordLinkage

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
    )[:1]
    df = pd.concat([pd.read_csv(f) for f in files]).reset_index(drop=True)
    for attr in settings.attributes:
        df[attr] = df[attr].astype(str)

    files2 = glob.glob(
        "/mnt/Research.CF/References & Training/Satchel/dedupe_rl/baseline_datasets/north_carolina_voters/*"
    )[1:2]
    df2 = pd.concat([pd.read_csv(f) for f in files2]).reset_index(drop=True)
    for attr in settings.attributes:
        df2[attr] = df2[attr].astype(str)

    df = df.sample(100_000, random_state=1234)
    df2 = df2.sample(100_000, random_state=1234)

    # %%
    d = RecordLinkage(settings=settings)
    d.initialize(df=df, df2=df2, reset=True)

    # %%
    d.fit_blocks()

    # %%
    res, res2 = d.predict()

    # %%
    res["cluster"].value_counts()