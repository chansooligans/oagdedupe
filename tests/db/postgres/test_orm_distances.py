""" integration testing postgres database initialization functions
"""
import os
import unittest

import pandas as pd
import pytest
from faker import Faker
from sqlalchemy import select

from oagdedupe.db.postgres.initialize import InitializeRepository
from oagdedupe.db.postgres.orm import DistanceRepository


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


def seed_distances(orm):
    data = [
        {"_index_l": 2, "_index_r": 2, "label": 1},
        {"_index_l": 1, "_index_r": 1, "label": 1},
    ]
    with orm.Session() as session:
        for d in data:
            row = orm.Comparisons(**d)
            row2 = orm.FullComparisons(**d)
            session.add(row)
            session.add(row2)
            session.commit()


class TestDistanceRepository(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, df, session):
        self.settings = settings
        self.session = session
        self.df = df
        self.df2 = df.copy()

    def setUp(self):
        self.init = InitializeRepository(settings=self.settings)
        self.init.setup(df=self.df, df2=self.df2, rl="")
        self.orm = DistanceRepository(settings=self.settings)
        seed_distances(orm=self.orm)
        return

    def test_get_attributes(self):
        self.orm.get_attributes(table=self.orm.Comparisons)
        df = pd.read_sql(select(self.orm.Comparisons), con=self.orm.engine)
        self.assertEqual(df["name_l"].isnull().sum(), 0)

    def test_compute_distances(self):
        self.orm.get_attributes(table=self.orm.Comparisons)
        self.orm.compute_distances(table=self.orm.Comparisons)
        df = pd.read_sql(select(self.orm.Comparisons), con=self.orm.engine)
        self.assertEqual(2, df.loc[0, self.settings.attributes].sum())
        self.assertEqual(any(df.loc[0].isnull()), False)

    def test_save_distances(self):
        self.orm.save_distances(full=False, labels=True)
        df = pd.read_sql(select(self.orm.Labels), con=self.orm.engine)
        self.assertEqual(14, len(df))
