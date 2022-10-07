import unittest

import pytest
from pytest import MonkeyPatch

from oagdedupe._typing import StatsDict
from oagdedupe.block.learner import Conjunctions
from oagdedupe.block.optimizers import DynamicProgram


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


class TestConjunctions(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_fixtures(self, settings, conjunctions):
        # https://stackoverflow.com/questions/22677654/why-cant-unittest-testcases-see-my-py-test-fixtures
        self.settings = settings
        self.conjunctions = conjunctions

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.cover = Conjunctions(
            settings=self.settings,
            optimizer=DynamicProgram(settings=self.settings),
        )
        return

    def test_conjunctions_list(self):
        with self.monkeypatch.context() as m:
            m.setattr(Conjunctions, "_conjunctions", self.conjunctions)
            res = self.cover.conjunctions_list
        self.assertEqual(res[0].rr, 0.99)
        self.monkeypatch.delattr(Conjunctions, "_conjunctions")
