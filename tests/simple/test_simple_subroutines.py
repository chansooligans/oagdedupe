"""
testing simple version subroutines
"""

from oagdedupe.simple.subroutines import get_signature, signatures_match
from oagdedupe.simple.concepts import Record, Scheme, Attribute
from oagdedupe.simple.schemes import FirstLetterFirstWord
from pytest import fixture
from typing import Tuple


@fixture
def record() -> Record:
    return Record({"name": "g", "address": "1"})

@fixture
def scheme() -> Scheme:
    class Fake(Scheme):
        def get_signature(record: Record, attribute: Attribute):
            return record.values[attribute]

        def signatures_match(sigs:Tuple) -> bool:
            return len(set(sigs)) == 1
    return Fake


def test_get_signature_no_exception(record, scheme):
    try:
        get_signature(record, "name", scheme)
    except Exception as e:
        assert False, e

def test_signatures_match_no_exception(record, scheme):
    try:
        signatures_match((record, record), "name", scheme)
    except Exception as e:
        assert False, e
