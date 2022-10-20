import os

import pytest

from oagdedupe._typing import StatsDict
from oagdedupe.settings import Settings, SettingsDB, SettingsModel


@pytest.fixture(scope="module")
def statsdict():
    return StatsDict(
        conjunction=tuple(["find_ngrams_4_postcode"]),
        n_pairs=7,
        positives=2,
        negatives=1,
        rr=8 / 15,
    )


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings(
        name="test",  # the name of the project, a unique identifier
        folder=".././.dedupe_test",  # path to folder where settings and data will be saved
        attributes=["name", "addr"],  # list of entity attribute names
        model=SettingsModel(
            dedupe=False,
            n=1000,
            k=3,
            max_compare=20_000,
            n_covered=5_000,
            cpus=20,  # parallelize distance computations
            path_model="./.dedupe/test_model",  # where to save the model
        ),
        db=SettingsDB(
            path_database=os.environ.get("DATABASE_URL"),
            db_schema="dedupe",
        ),
    )
