"""
testing simple version subroutines
"""

from oagdedupe.simple.subroutines import (
    get_pairs_one_conjunction,
    make_initial_labels,
)
from oagdedupe.simple.concepts import Record, Label
from oagdedupe.simple.schemes import first_letter_first_word
from pytest import fixture, mark


@fixture
def record() -> Record:
    return Record.from_dict({"name": "g", "address": "1"})


@fixture
def record2() -> Record:
    return Record.from_dict({"name": "g", "address": "211 some road"})


@mark.parametrize(
    "scheme,attribute,expected",
    [
        (first_letter_first_word, "name", True),
        (first_letter_first_word, "address", False),
    ],
)
def test_get_pairs_one_conjunction_works(
    record, record2, scheme, attribute, expected
):
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
