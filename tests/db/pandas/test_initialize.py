from pathlib import Path

from faker import Faker
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pytest import fixture

from oagdedupe.db.pandas.base import PandasInitializeRepository
from oagdedupe.settings import Settings, SettingsModel, SettingsDB


@fixture
def df() -> DataFrame:
    fake = Faker()
    fake.seed_instance(0)
    return DataFrame(
        {
            "name": [fake.name() for _ in range(10)],
            "addr": [fake.address() for _ in range(10)],
        }
    )


@fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        name="test",  # the name of the project, a unique identifier
        folder=str(
            tmp_path
        ),  # path to folder where settings and data will be saved
        attributes=["name", "addr"],  # list of entity attribute names
        model=SettingsModel(
            dedupe=False,
            n=100,
            k=3,
            max_compare=20_000,
            n_covered=5_000,
            cpus=20,  # parallelize distance computations
            path_model=str(tmp_path),  # where to save the model
        ),
        db=SettingsDB(
            path_database=str(tmp_path),
            db_schema="dedupe",
        ),
    )


@fixture
def pandas_init_repo(settings: Settings) -> PandasInitializeRepository:
    return PandasInitializeRepository(settings)


def test_setup(pandas_init_repo: PandasInitializeRepository, df: DataFrame):
    pandas_init_repo.setup(df)
    assert "_index" in pandas_init_repo.file_store.read("df")
    assert_frame_equal(
        df, pandas_init_repo.file_store.read("df").drop(columns=["_index"])
    )