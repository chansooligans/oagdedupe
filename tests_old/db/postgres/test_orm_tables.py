""" integration testing postgres database initialization functions
"""
import unittest

import pandas as pd
import pytest
from faker import Faker

from oagdedupe.db.postgres.initialize import InitializeRepository
from oagdedupe.db.postgres.tables import Tables


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
            row = orm.Labels(**d)
            session.add(row)
            session.commit()


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
    def prepare_fixtures(self, settings, df):
        self.settings = settings
        self.df = df
        self.df2 = df.copy()

    def setUp(self):
        self.init = InitializeRepository(settings=self.settings)
        self.init.setup(df=self.df, df2=self.df2, rl="")
        self.orm = Tables(settings=self.settings)
        seed_labels_distances(orm=self.orm)
        seed_distances(orm=self.orm)
        return

    def test__bulk_insert(self):
        newrow = pd.DataFrame(
            {"name": ["test"], "addr": ["test"], "_index": [-99]}
        )
        self.orm.engine.execute(
            f"TRUNCATE TABLE {self.settings.db.db_schema}.df_link"
        )
        self.orm.bulk_insert(newrow, self.init.maindf_link)
        df = pd.read_sql(
            f"SELECT * FROM {self.settings.db.db_schema}.df_link",
            con=self.orm.engine,
        )
        self.assertEqual(len(df), 1)
