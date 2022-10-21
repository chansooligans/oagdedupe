"""
testing simple version schemes
"""

from oagdedupe.simple.schemes import FirstLetterFirstWord
from oagdedupe.simple.concepts import Conjunction, Record, Scheme, Attribute
from oagdedupe.simple.schemes import FirstLetterFirstWord
from pytest import fixture, mark
from typing import Tuple


@fixture
def record() -> Record:
    return Record.from_dict({"name": "fake name", "address": "111 some road"})


@mark.parametrize(
    "scheme,attribute,expected",
    [
        (FirstLetterFirstWord, "name", "f"),
        (FirstLetterFirstWord, "address", "1"),
    ],
)
def test_get_signature_works(record, attribute, scheme, expected):
    assert scheme.get_signature(record, attribute) == expected


@mark.parametrize(
    "scheme,sigs,expected",
    [
        (FirstLetterFirstWord, ("a", "a"), True),
        (FirstLetterFirstWord, ("a", "b"), False),
    ],
)
def test_signatures_match_works(scheme, sigs, expected):
    assert scheme.signatures_match(sigs) is expected
