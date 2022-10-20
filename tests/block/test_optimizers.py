import os
import unittest
from dataclasses import dataclass

import pytest
from pytest import MonkeyPatch
from sqlalchemy import create_engine

from oagdedupe._typing import StatsDict
from oagdedupe.block.optimizers import DynamicProgram
from oagdedupe.db.base import BaseRepositoryBlocking


@pytest.fixture
def statslist():
    return [
        StatsDict(
            n_pairs=100,
            conjunction=tuple(["scheme"]),
            rr=0.9,
            positives=100,
            negatives=1,
        ),
        StatsDict(
            n_pairs=100,
            conjunction=tuple(["scheme"]),
            rr=0.99,
            positives=1,
            negatives=100,
        ),
    ]


def fake_sort(x):
    return StatsDict(
        n_pairs=10,
        conjunction=x,
        rr=0.999,
        positives=100,
        negatives=1,
    )


@dataclass
class FakeComputeBlocking:
    def get_conjunction_stats(self, conjunction, table):
        return StatsDict(
            n_pairs=10,
            conjunction=conjunction,
            rr=0.999,
            positives=100,
            negatives=1,
        )

    def max_key(self, x):
        return (x.rr, x.positives, -x.negatives)


class TestDynamicProgram(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, statsdict, statslist):
        self.settings = settings
        self.statsdict = statsdict
        self.statslist = statslist

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.optimizer = DynamicProgram(
            repo=FakeComputeBlocking(), settings=self.settings
        )
        return

    def test__keep_if(self):
        self.assertEqual(self.optimizer._keep_if(self.statsdict), True)

    def test__filter_and_sort(self):
        dp = self.optimizer._filter_and_sort(
            dp=self.statslist, n=1, scores=self.statslist
        )
        self.assertEqual(dp[-1].rr, 0.99)

    def test_get_best(self):
        res = self.optimizer.get_best(tuple(["scheme"]))
        self.assertEqual(len(res), 3)
        self.assertEqual(type(res[0]), StatsDict)
        self.assertEqual(res[-1].n_pairs, 10)
