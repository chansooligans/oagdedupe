""" integration testing postgres database initialization functions
"""
import unittest

import numpy as np
import pandas as pd
import pytest
from faker import Faker
from pytest import MonkeyPatch
from tqdm import tqdm

from oagdedupe.db.postgres.initialize import InitializeRepository
from oagdedupe.db.postgres.orm import FapiRepository


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
        {"name": 0.8, "addr": 0.8, "_index_l": 2, "_index_r": 2, "label": 1},
        {"name": 0.2, "addr": 0.2, "_index_l": 1, "_index_r": 1, "label": 1},
    ]
    with orm.Session() as session:
        for d in data:
            row = orm.Comparisons(**d)
            row2 = orm.FullComparisons(**d)
            session.add(row)
            session.add(row2)
            session.commit()


def fake_predict(self, dists):
    return [[0.1, 0.9], [0.85, 0.15]]


class TestFapiRepository(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, df):
        self.settings = settings
        self.df = df
        self.df2 = df.copy()

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.init = InitializeRepository(settings=self.settings)
        self.init.setup(df=self.df, df2=self.df2, rl="")
        self.orm = FapiRepository(settings=self.settings)
        seed_distances(orm=self.orm)
        return

    def test_get_distances(self):
        df = self.orm.get_distances()
        self.assertEqual(len(df), 2)

    def test_get_labels(self):
        df = self.orm.get_labels()
        self.assertEqual(len(df), 14)

    def test__update_table(self):
        newrow = pd.DataFrame(
            {"name": ["test"], "addr": ["test"], "_index": [-99]}
        )
        self.orm._update_table(newrow, self.init.maindf())
        df = pd.read_sql("SELECT * FROM dedupe.df", con=self.orm.engine)
        self.assertEqual(df.loc[100, "name"], "test")

    def test_update_train(self):
        unlabelled = pd.read_sql(
            "SELECT _index FROM dedupe.unlabelled LIMIT 2", con=self.orm.engine
        )["_index"]
        labelled = pd.read_sql(
            "SELECT _index FROM dedupe.pos LIMIT 2", con=self.orm.engine
        )["_index"]
        self.orm.update_train(
            newlabels=pd.DataFrame(
                {"_index_l": list(unlabelled), "_index_r": list(labelled)}
            )
        )
        df = pd.read_sql("SELECT * FROM dedupe.train", con=self.orm.engine)
        self.assertEqual(
            True, all(df.loc[df["_index"].isin(unlabelled), "labelled"] == True)
        )
        self.assertEqual(
            True, all(df.loc[df["_index"].isin(labelled), "labelled"] == True)
        )

    def test_save_predictions(self):

        with self.monkeypatch.context() as m:
            m.setattr(FapiRepository, "predict", fake_predict)
            self.orm.save_predictions()
        df = pd.read_sql("SELECT * FROM dedupe.scores", con=self.orm.engine)
        self.assertEqual([[0.9, 2, 2], [0.15, 1, 1]], df.values.tolist())
