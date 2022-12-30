"""
testing simple pandas version schemes
"""
from oagdedupe.simplepandas.schemes import FirstLetterFirstWord
from pytest import mark
from pandera.typing import Series
from pytest import fixture
from oagdedupe.simplepandas.concepts import Record
from pandera.typing import DataFrame
from pandas.testing import assert_series_equal


@fixture
def records() -> DataFrame[Record]:
    return DataFrame(
        [
            {"id": 1, "name": "g", "address": "1"},
            {"id": 2, "name": "g", "address": "211 some road"},
        ]
    )


@mark.parametrize(
    "scheme,value,expected",
    [
        (
            FirstLetterFirstWord,
            "testing this value",
            "t",
        ),
        (FirstLetterFirstWord, "123 some road", "1"),
    ],
)
def test_get(scheme, value, expected):
    assert scheme.get(value) == expected


def test_name_field():
    assert (
        FirstLetterFirstWord.name_field("address")
        == "FirstLetterFirstWord_address"
    )


def test_add(records: DataFrame[Record]):
    FirstLetterFirstWord.add(records, "address")
    assert_series_equal(
        records[FirstLetterFirstWord.name_field("address")],
        Series(["1", "2"]).rename(FirstLetterFirstWord.name_field("address")),
    )
