import unittest
import pytest
import os
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

db_url = os.environ.get("DATABASE_URL")
engine = create_engine(db_url)
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
def session():
    Base.metadata.create_all(engine)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings(
        name="default",
        folder=".dedupe_test",
        other=SettingsOther(
            n=5000,
            k=3,
            cpus=15,
            attributes=["name", "addr"],
            path_database="",
            db_schema="dedupe",
            path_model=".dedupe_test/test_model",
            label_studio=SettingsLabelStudio(
                port=8089,
                api_key="test_api_key",
                description="test project",
            ),
            fast_api=SettingsService(port=8003),
        ),
    )

class TestInitialize(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, df):
        # https://stackoverflow.com/questions/22677654/why-cant-unittest-testcases-see-my-py-test-fixtures
        self.settings = settings
        self.df = df

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.monkeypatch.setattr(Tables,"engine", engine)
        self.init = Initialize(settings=self.settings)
        self.init.setup_dynamic_declarative_mapping()
        self.init.reset_tables()
        return

    def test__init(self):
        self.init._init_df(df=self.df)
        df = pd.read_sql("SELECT * from dedupe.df", con=engine)
        print(df)