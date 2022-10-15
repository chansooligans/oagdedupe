""" integration testing postgres database initialization functions
"""
import os
import unittest

import pandas as pd
import pytest
from faker import Faker
from pytest import MonkeyPatch
from sqlalchemy import create_engine, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from oagdedupe.db.postgres.initialize import Initialize
from oagdedupe.db.postgres.orm import DatabaseORM
from oagdedupe.db.postgres.tables import Tables

db_url = os.environ.get("DATABASE_URL")
engine = create_engine(db_url)
Session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()


@pytest.fixture(scope="module")
def session():
    Base.metadata.create_all(engine)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(bind=engine)


def seed_maindf(orm):

    with orm.Session() as session:
        row = orm.maindf(**{"_index": 1})
        row2 = orm.maindf_link(**{"_index": 2})
        session.add(row)
        session.add(row2)
        session.commit()


def seed_clusters(orm):
    data = [
        {"cluster": 3, "_index": 1, "_type": True},
        {"cluster": 3, "_index": 2, "_type": False},
    ]
    with orm.Session() as session:
        for d in data:
            row = orm.Clusters(**d)
            session.add(row)
            session.commit()


class TestORM(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, session):
        # https://stackoverflow.com/questions/22677654/why-cant-unittest-testcases-see-my-py-test-fixtures
        self.settings = settings
        self.session = session

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.init = Initialize(settings=self.settings)
        self.init.engine = engine
        self.init.reset_tables()
        self.orm = DatabaseORM(settings=self.settings)
        seed_maindf(orm=self.orm)
        seed_clusters(orm=self.orm)
        return

    def test_get_clusters(self):
        df = self.orm.get_clusters()
        self.assertEqual(df["cluster"].values[0], 3)

    def test__cluster_subquery(self):
        sq = self.orm._cluster_subquery(session=self.session, _type=False)
        df = pd.read_sql(select(sq), con=engine)
        self.assertEqual(df["_index"].values[0], 2)

    def test_get_clusters_link(self):
        dflist = self.orm.get_clusters_link()
        self.assertEqual(dflist[0]["cluster"].values[0], 3)
        self.assertEqual(dflist[1]["cluster"].values[0], 3)
