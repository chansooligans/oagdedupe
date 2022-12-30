import unittest
from dataclasses import dataclass

import pytest
from pytest import MonkeyPatch

from oagdedupe._typing import StatsDict
from oagdedupe.block.learner import Conjunctions
from oagdedupe.db.base import BaseRepositoryBlocking


@pytest.fixture
def conjunctions():
    return [
        [
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
        ],
        [
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
        ],
        None,
    ]


@dataclass
class FakeOptimizer:
    repo = BaseRepositoryBlocking


class TestConjunctions(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, conjunctions):
        self.settings = settings
        self.conjunctions = conjunctions

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.cover = Conjunctions(
            settings=self.settings,
            optimizer=FakeOptimizer(),
        )
        return

    def test_conjunctions_list(self):
        with self.monkeypatch.context() as m:
            m.setattr(Conjunctions, "_conjunctions", self.conjunctions)
            m.setattr(
                BaseRepositoryBlocking,
                "max_key",
                lambda x: (x.rr, x.positives, -x.negatives),
            )
            res = self.cover.conjunctions_list
        self.assertEqual(res[0].rr, 0.99)
        self.monkeypatch.delattr(Conjunctions, "_conjunctions")
