import unittest
import pytest
from pytest import MonkeyPatch
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from faker import Faker
import pandas as pd

from oagdedupe.db.initialize import Initialize
from oagdedupe.db.tables import Tables
from oagdedupe.settings import (
    Settings,
    SettingsOther,
    SettingsService,
    SettingsLabelStudio,
)

engine = create_engine("postgresql+psycopg2://username:password@0.0.0.0:8000/db")
Session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()

@pytest.fixture(scope="module")
def df():
    fake = Faker()
    fake.seed_instance(0)
    return pd.DataFrame({
        'name': [fake.name() for x in range(100)],
        'addr': [fake.address() for x in range(100)]
    })

@pytest.fixture(scope="module")
def db_session():
    Base.metadata.create_all(engine)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="module")
def settings(tmp_path) -> Settings:
    return Settings(
        name="test",
        folder=tmp_path,
        other=SettingsOther(
            n=5000,
            k=3,
            cpus=15,
            attributes=["name", "addr"],
            path_database="test.db",
            db_schema="dedupe",
            path_model=tmp_path / "test_model",
            label_studio=SettingsLabelStudio(
                port=8089,
                api_key="test_api_key",
                description="test project",
            ),
            fast_api=SettingsService(port=8003),
        ),
    )

def test_connection(db_session):
    res = db_session.query(text("1"))
    assert res.all() == [(1,)]


class TestInitialize(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, df):
        # https://stackoverflow.com/questions/22677654/why-cant-unittest-testcases-see-my-py-test-fixtures
        self.settings = settings
        self.df = df
        self.monkeypatch.setattr(Tables,"engine", engine)

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.init = Initialize(settings=self.settings)
        return

    def test__init_df(self):
        self.init._init_df(self.df)
