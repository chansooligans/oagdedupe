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
from oagdedupe.db.tables import Tables

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


class FixtureMixin:
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, df, session):
        # https://stackoverflow.com/questions/22677654/why-cant-unittest-testcases-see-my-py-test-fixtures
        self.settings = settings
        self.df = df
        self.df2 = df.copy()
        self.session = session


class TestDF(unittest.TestCase, FixtureMixin):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.monkeypatch.setattr(Tables, "engine", engine)
        self.init = Initialize(settings=self.settings)
        self.init.reset_tables()
        return

    def test__init_df(self):
        self.init._init_df(df=self.df, df_link=self.df2)
        df = pd.read_sql("SELECT * from dedupe.df", con=engine)
        self.assertEqual(len(df), 100)


class TestPosNegUnlabelled(unittest.TestCase, FixtureMixin):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.monkeypatch.setattr(Tables, "engine", engine)
        self.init = Initialize(settings=self.settings)
        self.init.reset_tables()
        self.init._init_df(df=self.df, df_link=self.df2)
        return

    def test__init_pos(self):
        self.init._init_pos(self.session)
        df = pd.read_sql("SELECT * from dedupe.pos", con=engine)
        self.assertEqual(len(df), 4)

    def test__init_neg(self):
        self.init._init_neg(self.session)
        df = pd.read_sql("SELECT * from dedupe.neg", con=engine)
        self.assertEqual(len(df), 10)

    def test__init_unlabelled(self):
        self.init._init_unlabelled(self.session)
        df = pd.read_sql("SELECT * from dedupe.unlabelled", con=engine)
        self.assertEqual(len(df), 100)


class TestTrainLabels(unittest.TestCase, FixtureMixin):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.monkeypatch.setattr(Tables, "engine", engine)
        self.init = Initialize(settings=self.settings)
        self.init.reset_tables()
        self.init._init_df(df=self.df, df_link=self.df2)
        self.init._init_pos(self.session)
        self.init._init_neg(self.session)
        self.init._init_unlabelled(self.session)
        return

    def test__init_train(self):
        self.init._init_train(self.session)
        df = pd.read_sql("SELECT * from dedupe.train", con=engine)
        assert len(df) >= 103

    def test__init_labels(self):
        self.init._init_labels(self.session)
        df = pd.read_sql("SELECT * from dedupe.labels", con=engine)
        assert len(df) > 10

    def test__init_labels_link(self):
        self.init._init_labels_link(self.session)
        df = pd.read_sql("SELECT * from dedupe.labels", con=engine)
        assert len(df) > 10
