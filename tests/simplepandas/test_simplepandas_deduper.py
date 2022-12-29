from oagdedupe.simplepandas.deduper import Deduper
from oagdedupe.simplepandas.concepts import Entity, Pair
from oagdedupe.simplepandas.fakes import (
    FakeConjunctionFinder,
    FakeClusterer,
    FakeClassifier,
    FakeActiveLearner,
)
from oagdedupe.simplepandas.repositories import (
    InMemoryLabelRepository,
)
from pytest import fixture
from pandera.typing import DataFrame
from pandas.testing import assert_frame_equal


@fixture
def records() -> DataFrame:
    return DataFrame(
        [
            {"id": 1, "name": "g", "address": "1"},
            {"id": 2, "name": "g", "address": "211 some road"},
        ]
    )


@fixture
def pair(record, record2) -> Pair:
    return frozenset({record, record2})


@fixture
def label_repo_same(pair) -> InMemoryLabelRepository:
    label_repo = InMemoryLabelRepository()
    label_repo.add(pair, True)
    return label_repo


def test_dedupe_single_label_runs(records):
    deduper = Deduper(
        records=records,
        attributes={"name", "address"},
        conj_finder=FakeConjunctionFinder(),
        label_repo=InMemoryLabelRepository(),
        classifier=FakeClassifier(),
        clusterer=FakeClusterer(),
        active_learner=FakeActiveLearner(),
    )

    assert_frame_equal(
        deduper.entities,
        DataFrame(
            [
                {"id": 1, "entity_id": 0},
                {"id": 2, "entity_id": 0},
            ]
        ),
    )
