Record Linkage Example
=====

.. code-block:: python

    from oagdedupe.settings import (
        Settings,
        SettingsOther,
    )
    from oagdedupe.api import RecordLinkage

    import glob
    import pandas as pd
    pd.options.display.precision = 12
    from sqlalchemy import create_engine
    engine = create_engine("postgresql+psycopg2://username:password@0.0.0.0:8000/db")

    # %%
    settings = Settings(
        name="default",  # the name of the project, a unique identifier
        folder="./.dedupe",  # path to folder where settings and data will be saved
        other=SettingsOther(
            dedupe=False,
            n=5000,
            k=3,
            cpus=20,  # parallelize distance computations
            attributes=["givenname", "surname", "suburb", "postcode"],  # list of entity attribute names
            path_database="postgresql+psycopg2://username:password@0.0.0.0:8000/db",  # where to save the sqlite database holding intermediate data
            db_schema="dedupe_rl",
            path_model="./.dedupe/test_model_rl",  # where to save the model
            label_studio={
                "port": 8089,  # label studio port
                "api_key": "83e2bc3da92741aa41c272829558c596faefa745",  # label studio port
                "description": "chansoo test project",  # label studio description of project
            },
            fast_api={"port": 8090},  # fast api port
        ),
    )
    settings.save()

    # %%
    files = glob.glob(
        "/mnt/Research.CF/References & Training/Satchel/dedupe_rl/baseline_datasets/north_carolina_voters/*"
    )[:1]
    df = pd.concat([pd.read_csv(f) for f in files]).reset_index(drop=True)
    for attr in settings.other.attributes:
        df[attr] = df[attr].astype(str)

    files2 = glob.glob(
        "/mnt/Research.CF/References & Training/Satchel/dedupe_rl/baseline_datasets/north_carolina_voters/*"
    )[1:2]
    df2 = pd.concat([pd.read_csv(f) for f in files2]).reset_index(drop=True)
    for attr in settings.other.attributes:
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