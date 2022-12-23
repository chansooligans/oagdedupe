from typing import FrozenSet
from oagdedupe.simplepandas.deduper import Deduper
from oagdedupe.simplepandas.concepts import Entity, Pair, Record, Label
from oagdedupe.simplepandas.fakes import (
    FakeConjunctionFinder,
    FakeClusterer,
    fake_classifier,
)
from oagdedupe.simplepandas.repositories import (
    InMemoryLabelRepository,
    InMemoryClassifierRepository,
)
from pytest import fixture
from pandera.typing import DataFrame


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
    label_repo.add(pair, Label.SAME)
    return label_repo


def test_dedupe_single_label_runs(records):
    deduper = Deduper(
        records=records,
        name_field_id = "id",
        attributes={"name", "address"},
        conj_finder=FakeConjunctionFinder(),
        labels={pair: Label.SAME},
        classifier_repo=InMemoryClassifierRepository(),
        clusterer=FakeClusterer(),
    )

    assert Entity(records=records) in deduper.get_entities()
