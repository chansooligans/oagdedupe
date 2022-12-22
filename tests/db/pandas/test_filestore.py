from pathlib import Path
from oagdedupe.db.pandas.base import FileStore
from pytest import fixture
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from faker import Faker


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
def file_store(tmp_path: Path) -> FileStore:
    return FileStore(tmp_path)


def test_save(tmp_path: Path, df: DataFrame):
    assert not (tmp_path / "df.csv").is_file()
    file_store = FileStore(tmp_path)
    file_store.save(df, "df")
    assert (tmp_path / "df.csv").is_file()


def test_read(file_store: FileStore, df: DataFrame):
    file_store.save(df, "df")
    assert_frame_equal(file_store.read("df"), df)


def test_names(file_store: FileStore, df: DataFrame):
    assert "df" not in file_store.names
    file_store.save(df, "df")
    assert "df" in file_store.names
    file_store.fp("df").unlink()
    assert "df" not in file_store.names
