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

from oagdedupe.block.sql import LearnerSql
from oagdedupe.db.initialize import Initialize
from oagdedupe.settings import Settings, SettingsOther

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


def seed_labels():
    engine.execute(
        """  
        DROP TABLE IF EXISTS dedupe.labels;
        CREATE TABLE IF NOT EXISTS dedupe.labels(_index_l int, _index_r int, label int);
        INSERT INTO dedupe.labels (_index_l, _index_r, label) 
        VALUES 
            (0, 0, 1),
            (0, 1, 1),
            (1, 1, 0);
    """
    )


class TestSQL(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings):
        # https://stackoverflow.com/questions/22677654/why-cant-unittest-testcases-see-my-py-test-fixtures
        self.settings = settings

    def setUp(self):
        self.init = Initialize(settings=self.settings)
        self.init.reset_tables()
        self.sql = LearnerSql(settings=self.settings)
        seed_blocks_train()
        seed_labels()
        return

    def test__inv_idx_query(self):
        query = self.sql._inv_idx_query(
            names=["find_ngrams_4_postcode"], table="blocks_train"
        )
        df = pd.read_sql(query, con=engine)
        self.assertEqual(len(df), 6)

    def test__inv_idx_query_link(self):
        query = self.sql._inv_idx_query(
            names=["find_ngrams_4_postcode"],
            table="blocks_train_link",
            col="_index_r",
        )
        df = pd.read_sql(query, con=engine)
        self.assertEqual(len(df), 6)

    def test_get_inverted_index_stats(self):
        res = self.sql.get_inverted_index_stats(
            names=["find_ngrams_4_postcode"], table="blocks_train"
        )
        self.assertDictEqual(
            res, {"n_pairs": 7, "positives": 2, "negatives": 1}
        )

    def test_save_comparison_pairs(self):
        self.sql.save_comparison_pairs(
            names=["find_ngrams_4_postcode"], table="blocks_train"
        )
        df = pd.read_sql("SELECT * FROM dedupe.comparisons", con=engine)
        self.assertEqual(len(df), 7)
