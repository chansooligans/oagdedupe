import unittest

import pytest
from pytest import MonkeyPatch, fixture

from oagdedupe._typing import StatsDict
from oagdedupe.block.optimizers import DynamicProgram
from oagdedupe.block.sql import LearnerSql


@pytest.fixture
def stats():
    return StatsDict(
        n_pairs=10,
        scheme=tuple(["scheme"]),
        rr=0.999,
        positives=100,
        negatives=1,
    )


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


class TestDynamicProgram(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, stats, statslist, conjunctions):
        # https://stackoverflow.com/questions/22677654/why-cant-unittest-testcases-see-my-py-test-fixtures
        self.settings = settings
        self.stats = stats
        self.statslist = statslist
        self.conjunctions = conjunctions

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.optimzier = DynamicProgram(
            settings=self.settings, db=LearnerSql(settings=self.settings)
        )
        return

    def test_scheme_stats(self):
        def mockstats(*args, **kwargs):
            return StatsDict(
                n_pairs=10,
                scheme=tuple(["scheme"]),
                rr=0.999,
                positives=100,
                negatives=1,
            )

        with self.monkeypatch.context() as m:
            m.setattr(LearnerSql, "get_inverted_index_stats", mockstats)
            m.setattr(LearnerSql, "n_comparisons", 10_000)

            res = self.optimzier.scheme_stats(
                names=tuple(["scheme"]), table="table"
            )

        self.assertEqual(res, self.stats)

    def test__keep_if(self):
        self.assertEqual(self.optimzier._keep_if(self.stats), True)

    def test__filter_and_sort(self):
        dp = self.optimzier._filter_and_sort(
            dp=self.statslist, n=1, scores=self.statslist
        )
        self.assertEqual(dp[-1].rr, 0.99)
