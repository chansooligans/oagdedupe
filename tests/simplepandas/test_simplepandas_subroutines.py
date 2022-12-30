"""
testing simple pandas version subroutines
"""

from oagdedupe.simplepandas.subroutines import (
    get_signatures,
    get_pairs_one_conjunction,
    get_pairs_limit_pairs,
    get_pairs_limit_conjunctions,
    get_initial_labels,
)
from pytest import fixture, mark
from oagdedupe.simplepandas.concepts import Record, Conjunction, Pair, Scheme
from pandera.typing import DataFrame, Series
from oagdedupe.simplepandas.schemes import FirstLetterFirstWord
from pandas.testing import assert_series_equal, assert_frame_equal
from pandas import RangeIndex


@fixture
def records() -> DataFrame[Record]:
    return DataFrame(
        [
            {"id": 1, "name": "g", "address": "1"},
            {"id": 2, "name": "g", "address": "211 some road"},
        ]
    )


@mark.parametrize(
    ["conjunction", "scheme", "attribute", "expected"],
    [
        (
            {(FirstLetterFirstWord, "address")},
            FirstLetterFirstWord,
            "address",
            Series(["1", "2"]),
        ),
        (
            {(FirstLetterFirstWord, "name")},
            FirstLetterFirstWord,
            "name",
            Series(["g", "g"]),
        ),
    ],
)
def test_get_signatures(
    records: DataFrame[Record],
    conjunction: Conjunction,
    scheme: Scheme,
    attribute: str,
    expected: Series,
):
    get_signatures(records=records, conjunction=conjunction)
    assert_series_equal(
        records[scheme.name_field(attribute)],
        expected.rename(scheme.name_field(attribute)),
    )


@mark.parametrize(
    ["conjunction", "expected"],
    [
        (
            {
                (FirstLetterFirstWord, "address"),
            },
            Pair.example(size=0),
        ),
        (
            {
                (FirstLetterFirstWord, "name"),
            },
            DataFrame(
                [{"id1": 1, "id2": 2}],
                index=[1],
            ),
        ),
    ],
)
def test_get_pairs_one_conjunction(
    records: DataFrame[Record],
    conjunction: Conjunction,
    expected: DataFrame[Pair],
):
    assert_frame_equal(
        expected.reindex(),
        get_pairs_one_conjunction(records=records, conjunction=conjunction),
    )


def test_get_pairs_limit_conjunctions(records: DataFrame[Record]):
    assert_frame_equal(
        DataFrame(
            [{"id1": 1, "id2": 2}],
            index=[1],
        ),
        get_pairs_limit_conjunctions(
            records,
            (
                {
                    (FirstLetterFirstWord, "name"),
                },
            ),
            limit=1,
        ),
    )
