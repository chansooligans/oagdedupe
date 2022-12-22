from typing import FrozenSet
from oagdedupe.simple.deduper import Deduper
from oagdedupe.simple.concepts import Entity, Pair, Record, Label
from oagdedupe.simple.fakes import (
    FakeConjunctionFinder,
    FakeClusterer,
    fake_classifier,
)
from oagdedupe.simple.repositories import (
    InMemoryLabelRepository,
    InMemoryClassifierRepository,
)
from pytest import fixture


@fixture
def record() -> Record:
    return Record.from_dict({"name": "g", "address": "1"})


@fixture
def record2() -> Record:
    return Record.from_dict({"name": "g", "address": "211 some road"})


@fixture
def records(record, record2) -> FrozenSet[Record]:
    return frozenset({record, record2})


@fixture
def pair(record, record2) -> Pair:
    return frozenset({record, record2})


@fixture
def label_repo_same(pair) -> InMemoryLabelRepository:
    label_repo = InMemoryLabelRepository()
    label_repo.add(pair, Label.SAME)
    return label_repo


def test_dedupe_no_labels_runs(record, records):
    deduper = Deduper(
        records=records,
        attributes={"name", "address"},
        conj_finder=FakeConjunctionFinder(),
        label_repo=InMemoryLabelRepository(),
        classifier_repo=InMemoryClassifierRepository(),
        clusterer=FakeClusterer(),
    )

    assert Entity(records=frozenset({record})) in deduper.get_entities()
