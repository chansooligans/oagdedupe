""" integration testing postgres database initialization functions
"""
import os
import unittest
from typing import Tuple

import pandas as pd
import pytest
from pytest import MonkeyPatch
from sqlalchemy import create_engine

from oagdedupe.block.mixin import ConjunctionMixin
from oagdedupe.db.initialize import Initialize

db_url = os.environ.get("DATABASE_URL")
engine = create_engine(db_url)


def seed_blocks_train():
    engine.execute(
        """
        CREATE TABLE IF NOT EXISTS
            dedupe.blocks_train(_index INT, find_ngrams_4_postcode text[]);

        CREATE TABLE IF NOT EXISTS
            dedupe.blocks_train_link(_index INT, find_ngrams_4_postcode text[]);

        INSERT INTO dedupe.blocks_train (_index, find_ngrams_4_postcode)
        VALUES
            (0, '{"abcd","efgh"}'),
            (1, '{"abcd","xyzw"}'),
            (2, '{"opqr","xyzw"}');

        INSERT INTO dedupe.blocks_train_link (_index, find_ngrams_4_postcode)
        VALUES
            (0, '{"abcd","efgh"}'),
            (1, '{"abcd","xyzw"}'),
            (2, '{"opqr","xyzw"}')
    """
    )


class TestMixin(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, statsdict):
        # https://stackoverflow.com/questions/22677654/why-cant-unittest-testcases-see-my-py-test-fixtures
        self.settings = settings
        self.statsdict = statsdict

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.init = Initialize(settings=self.settings)
        self.init.reset_tables()
        self.mixin = ConjunctionMixin(settings=self.settings)
        seed_blocks_train()
        return

    def test__inv_idx_query(self):
        query = self.mixin._inv_idx_query(
            names=["find_ngrams_4_postcode"], table="blocks_train"
        )
        df = pd.read_sql(query, con=engine)
        self.assertEqual(len(df), 6)

    def test__inv_idx_query_link(self):
        query = self.mixin._inv_idx_query(
            names=["find_ngrams_4_postcode"],
            table="blocks_train_link",
            col="_index_r",
        )
        df = pd.read_sql(query, con=engine)
        self.assertEqual(len(df), 6)
