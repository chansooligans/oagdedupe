""" integration testing postgres database initialization functions
"""
import os
import unittest

import pandas as pd
import pytest
from faker import Faker
from pytest import MonkeyPatch
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from oagdedupe.db.initialize import Initialize
from oagdedupe.db.orm import DatabaseORM
from oagdedupe.db.tables import Tables
from oagdedupe.settings import Settings, SettingsOther

db_url = os.environ.get("DATABASE_URL")
engine = create_engine(db_url)
Session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()


@pytest.fixture(scope="module")
def df():
    fake = Faker()
    fake.seed_instance(0)
    return pd.DataFrame(
        {
            "name": [fake.name() for x in range(100)],
            "addr": [fake.address() for x in range(100)],
        }
    )


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
        name="default",  # the name of the project, a unique identifier
        folder="./.dedupe_test",  # path to folder where settings and data will be saved
        other=SettingsOther(
            dedupe=False,
            n=5000,
            k=3,
            max_compare=20_000,
            n_covered=5_000,
            cpus=20,  # parallelize distance computations
            attributes=["name", "addr"],  # list of entity attribute names
            path_database=os.environ.get(
                "DATABASE_URL"
            ),  # where to save the sqlite database holding intermediate data
            db_schema="dedupe",
            path_model="./.dedupe_test/test_model",  # where to save the model
            label_studio={
                "port": 8089,  # label studio port
                "api_key": "33344e8a477f8adc3eb6aa1e41444bde76285d96",  # label studio port
                "description": "chansoo test project",  # label studio description of project
            },
            fast_api={"port": 8090},  # fast api port
        ),
    )


def seed_labels_distances(orm):
    data = [
        {"_index_l": 1, "_index_r": 1, "label": None},
        {"_index_l": 2, "_index_r": 2, "label": 1},
    ]
    with orm.Session() as session:
        for d in data:
            row = orm.LabelsDistances(**d)
            session.add(row)
            session.commit()


def seed_distances(orm):
    data = [
        {"_index_l": 2, "_index_r": 2, "label": 1},
        {"_index_l": 1, "_index_r": 1, "label": 1},
    ]
    with orm.Session() as session:
        for d in data:
            row = orm.Distances(**d)
            row2 = orm.FullDistances(**d)
            session.add(row)
            session.commit()
            session.add(row2)
            session.commit()


class TestORM(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, df, session):
        # https://stackoverflow.com/questions/22677654/why-cant-unittest-testcases-see-my-py-test-fixtures
        self.settings = settings
        self.df = df
        self.df2 = df.copy()
        self.session = session

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.monkeypatch.setattr(Tables, "engine", engine)
        self.init = Initialize(settings=self.settings)
        self.init.setup(
            df=self.df, df2=self.df2, reset=True, resample=False, rl=""
        )
        self.orm = DatabaseORM(settings=self.settings)
        seed_labels_distances(orm=self.orm)
        seed_distances(orm=self.orm)
        return

    def test_get_train(self):
        df = self.orm.get_train()
        assert len(df) >= 103

    def test_get_labels(self):
        df = self.orm.get_labels()
        self.assertEqual(len(df), 2)

    def test_get_distances(self):
        df = self.orm.get_distances()
        self.assertEqual(len(df), 1)

    def test_get_full_distances(self):
        df = self.orm.get_full_distances()
        self.assertEqual(len(df), 2)

    def test_get_full_comparison_indices(self):
        df = self.orm.get_full_comparison_indices()
        self.assertEqual(df.loc[0, "_index_l"], 1)

    def test_compare_cols(self):
        self.assertEqual(
            self.orm.compare_cols,
            ["name_l", "addr_l", "name_r", "addr_r", "_index_l", "_index_r"],
        )

    def test__update_table(self):
        newrow = pd.DataFrame(
            {"name": ["test"], "addr": ["test"], "_index": [-99]}
        )
        self.orm._update_table(newrow, self.init.maindf())
        df = pd.read_sql("SELECT * FROM dedupe.df", con=engine)
        self.assertEqual(df.loc[100, "name"], "test")

    def test__bulk_insert(self):
        newrow = pd.DataFrame(
            {"name": ["test"], "addr": ["test"], "_index": [-99]}
        )
        engine.execute("TRUNCATE TABLE dedupe.df_link")
        self.orm.bulk_insert(newrow, self.init.maindf_link)
        df = pd.read_sql("SELECT * FROM dedupe.df_link", con=engine)
        self.assertEqual(len(df), 1)
