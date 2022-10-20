""" integration testing postgres database initialization functions
"""
import os
import unittest

import pandas as pd
import pytest
from faker import Faker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from oagdedupe.db.postgres.initialize import InitializeRepository
from oagdedupe.db.postgres.tables import Tables


@pytest.fixture(scope="module")
def df():
    fake = Faker()
    fake.seed_instance(0)
    return pd.DataFrame(
        {
            "name": [fake.name() for x in range(200)],
            "addr": [fake.address() for x in range(200)],
        }
    )


class FixtureMixin:
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, df, session, engine):
        self.settings = settings
        self.engine = engine
        self.df = df
        self.df2 = df.copy()
        self.session = session


class TestDF(unittest.TestCase, FixtureMixin):
    def setUp(self):
        self.init = InitializeRepository(settings=self.settings)
        self.init.engine = self.engine
        self.init.reset_tables()
        return

    def test__init_df(self):
        self.init._init_df(df=self.df, df_link=self.df2)
        df = pd.read_sql("SELECT * from dedupe.df", con=self.engine)
        self.assertEqual(len(df), 200)


class TestPosNegUnlabelled(unittest.TestCase, FixtureMixin):
    def setUp(self):
        self.init = InitializeRepository(settings=self.settings)
        self.init.reset_tables()
        self.init.engine = self.engine
        self.init._init_df(df=self.df, df_link=self.df2)
        return

    def test__init_pos(self):
        self.init._init_pos(self.session)
        df = pd.read_sql("SELECT * from dedupe.pos", con=self.engine)
        self.assertEqual(len(df), 4)

    def test__init_neg(self):
        self.init._init_neg(self.session)
        df = pd.read_sql("SELECT * from dedupe.neg", con=self.engine)
        self.assertEqual(len(df), 10)

    def test__init_unlabelled(self):
        self.init._init_unlabelled(self.session)
        df = pd.read_sql("SELECT * from dedupe.unlabelled", con=self.engine)
        self.assertEqual(len(df), 100)


class TestTrainLabels(unittest.TestCase, FixtureMixin):
    def setUp(self):
        self.init = InitializeRepository(settings=self.settings)
        self.init.engine = self.engine
        self.init.reset_tables()
        self.init._init_df(df=self.df, df_link=self.df2)
        self.init._init_pos(self.session)
        self.init._init_neg(self.session)
        self.init._init_unlabelled(self.session)
        return

    def test__init_train(self):
        self.init._init_train(self.session)
        df = pd.read_sql("SELECT * from dedupe.train", con=self.engine)
        assert len(df) >= 103

    def test__init_labels(self):
        self.init._init_labels(self.session)
        df = pd.read_sql("SELECT * from dedupe.labels", con=self.engine)
        assert len(df) > 10

    def test__init_labels_link(self):
        self.init._init_labels_link(self.session)
        df = pd.read_sql("SELECT * from dedupe.labels", con=self.engine)
        assert len(df) > 10


class TestResample(unittest.TestCase, FixtureMixin):
    def setUp(self):
        self.init = InitializeRepository(settings=self.settings)
        self.init.engine = self.engine
        self.init.setup(df=self.df, df2=self.df2)
        return

    def test_delete_unlabelled_from_train(self):
        df_old = pd.read_sql("SELECT * from dedupe.train", con=self.engine)
        self.init._delete_unlabelled_from_train(session=self.session)
        df_new = pd.read_sql("SELECT * from dedupe.train", con=self.engine)
        self.assertEqual(df_old["labelled"].sum(), len(df_new))

    def test__truncate_unlabelled(self):
        self.init._truncate_unlabelled()
        df = pd.read_sql("SELECT * from dedupe.unlabelled", con=self.engine)
        self.assertEqual(len(df), 0)

    def test__init_unlabelled(self):
        self.init._truncate_unlabelled()
        self.init._init_unlabelled(session=self.session)
        df = pd.read_sql("SELECT * from dedupe.unlabelled", con=self.engine)
        self.assertEqual(len(df), 100)

    def test_resample(self):
        old = pd.read_sql(
            "SELECT * from dedupe.train ORDER BY _index", con=self.engine
        )["_index"].values
        self.init._delete_unlabelled_from_train(session=self.session)
        new = pd.read_sql(
            "SELECT * from dedupe.train ORDER BY _index", con=self.engine
        )["_index"].values
        self.assertEqual(False, list(old) == list(new))
