"""
testing simple version subroutines
"""

from oagdedupe.simple.subroutines import (
    get_signature,
    signatures_match,
    get_pairs,
)
from oagdedupe.simple.concepts import Record, Scheme, Attribute
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
    records = {record, record2}
    assert (
        {record, record2} in get_pairs(records, {(scheme, attribute)})
    ) is expected
