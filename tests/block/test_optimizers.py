import os
import unittest

import pytest
from pytest import MonkeyPatch
from sqlalchemy import create_engine

from oagdedupe._typing import StatsDict
from oagdedupe.block.mixin import ConjunctionMixin
from oagdedupe.block.optimizers import DynamicProgram
from oagdedupe.containers import Container
from oagdedupe.db.initialize import Initialize

db_url = os.environ.get("DATABASE_URL")
engine = create_engine(db_url)


@pytest.fixture
def statslist():
    return [
        StatsDict(
            n_pairs=100,
            scheme=tuple(["scheme"]),
            rr=0.9,
            positives=100,
            negatives=1,
        ),
        StatsDict(
            n_pairs=100,
            scheme=tuple(["scheme"]),
            rr=0.99,
            positives=1,
            negatives=100,
        ),
    ]


@pytest.fixture
def conjunctions():
    return [
        [
            StatsDict(
                n_pairs=100,
                scheme=tuple(["scheme"]),
                rr=0.9,
                positives=100,
                negatives=1,
            ),
            StatsDict(
                n_pairs=100,
                scheme=tuple(["scheme"]),
                rr=0.99,
                positives=1,
                negatives=100,
            ),
        ],
        [
            StatsDict(
                n_pairs=100,
                scheme=tuple(["scheme"]),
                rr=0.9,
                positives=100,
                negatives=1,
            ),
            StatsDict(
                n_pairs=100,
                scheme=tuple(["scheme"]),
                rr=0.99,
                positives=1,
                negatives=100,
            ),
        ],
        None,
    ]


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


class TestDynamicProgram(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, statsdict, statslist, conjunctions):
        # https://stackoverflow.com/questions/22677654/why-cant-unittest-testcases-see-my-py-test-fixtures
        self.settings = settings
        self.statsdict = statsdict
        self.statslist = statslist
        self.conjunctions = conjunctions

    def setUp(self):
        container = Container()
        container.settings.override(self.settings)
        self.monkeypatch = MonkeyPatch()
        self.optimizer = DynamicProgram()
        self.init = Initialize()
        self.init.reset_tables()
        seed_blocks_train()
        seed_labels()
        return

    def test__keep_if(self):
        self.assertEqual(self.optimizer._keep_if(self.statsdict), True)

    def test__filter_and_sort(self):
        dp = self.optimizer._filter_and_sort(
            dp=self.statslist, n=1, scores=self.statslist
        )
        self.assertEqual(dp[-1].rr, 0.99)

    def test_get_inverted_index_stats(self):
        with self.monkeypatch.context() as m:
            m.setattr(DynamicProgram, "n_comparisons", 15)
            res = self.optimizer.get_inverted_index_stats(
                names=tuple(["find_ngrams_4_postcode"]), table="blocks_train"
            )
        self.assertEqual(res, self.statsdict)

    def test_get_best(self):
        def mockstats(*args, **kwargs):
            return StatsDict(
                n_pairs=10,
                scheme=tuple(["scheme"]),
                rr=0.999,
                positives=100,
                negatives=1,
            )

        with self.monkeypatch.context() as m:
            m.setattr(DynamicProgram, "score", mockstats)
            m.setattr(
                ConjunctionMixin, "blocking_schemes", list(tuple(["scheme"]))
            )
            res = self.optimizer.get_best(tuple(["scheme"]))
        self.assertEqual(res[0], mockstats())
