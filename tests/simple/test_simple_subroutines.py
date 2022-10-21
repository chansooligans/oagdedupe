"""
testing simple version subroutines
"""

from oagdedupe.simple.subroutines import (
    get_signature,
    signatures_match,
    get_pairs_one_conjunction,
    make_initial_labels,
)
from oagdedupe.simple.concepts import Record, Scheme, Attribute, Label
from oagdedupe.simple.schemes import FirstLetterFirstWord
from pytest import fixture, mark
from typing import Tuple, Type


@fixture
def record() -> Record:
    return Record.from_dict({"name": "g", "address": "1"})


@fixture
def record2() -> Record:
    return Record.from_dict({"name": "g", "address": "211 some road"})


@fixture
def scheme() -> Type[Scheme]:
    class Fake(Scheme):
        @staticmethod
        def get_signature(record: Record, attribute: Attribute):
            return record.values[attribute]

        @staticmethod
        def signatures_match(sigs: Tuple) -> bool:
            return len(set(sigs)) == 1

    return Fake


def test_get_signature_no_exception(record, scheme):
    try:
        get_signature(record, "name", scheme)
    except Exception as e:
        assert False, e


def test_signatures_match_no_exception(record, scheme):
    try:
        signatures_match({record, record}, "name", scheme)
    except Exception as e:
        assert False, e


@mark.parametrize(
    "scheme,attribute,expected",
    [
        (FirstLetterFirstWord, "name", True),
        (FirstLetterFirstWord, "address", False),
    ],
)
def test_get_pairs_works(record, record2, scheme, attribute, expected):
    records = frozenset({record, record2})
    assert (
        frozenset({record, record2})
        in get_pairs_one_conjunction(records, {(scheme, attribute)})
    ) is expected


def test_make_initial_labels_works(record, record2):
    labels = make_initial_labels(frozenset({record, record2}))
    assert labels[frozenset({record, record})] == Label.SAME
    assert labels[frozenset({record, record2})] == Label.NOT_SAME
    assert labels[frozenset({record2, record})] == Label.NOT_SAME
