""" integration testing postgres database initialization functions
"""
import unittest

import pandas as pd
import pytest
from faker import Faker

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
            session.add(row2)
            session.commit()


class TestDistanceRepository(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, df):
        self.settings = settings
        self.df = df
        self.df2 = df.copy()

    def setUp(self):
        self.init = InitializeRepository(settings=self.settings)
        self.init.setup(
            df=self.df, df2=self.df2, reset=True, resample=False, rl=""
        )
        self.orm = DistanceRepository(settings=self.settings)
        seed_labels_distances(orm=self.orm)
        seed_distances(orm=self.orm)
        return

    def test_get_distances(self):
        df = self.orm.get_distances()
        self.assertEqual(len(df), 1)
