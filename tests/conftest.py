import os

import pytest

from oagdedupe.settings import Settings, SettingsOther


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings(
        name="test",  # the name of the project, a unique identifier
        folder=".././.dedupe_test",  # path to folder where settings and data will be saved
        other=SettingsOther(
            dedupe=False,
            n=1000,
            k=3,
            max_compare=20_000,
            n_covered=5_000,
            cpus=1,  # parallelize distance computations
            attributes=["name", "addr"],  # list of entity attribute names
            path_database=os.environ.get("DATABASE_URL"),
            db_schema="dedupe",
        ),
    )
