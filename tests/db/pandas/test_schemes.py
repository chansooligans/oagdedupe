from pytest import mark
from oagdedupe.db.pandas.schemes import Scheme


@mark.parametrize(
    ["value", "n", "expected"],
    [
        ("abcdefg", 2, "ab"),
        ("abcdefg", 4, "abcd"),
        ("abcdefg", 6, "abcdef"),
        ("a", 2, "a"),
    ],
)
def test_first_nchars(value: str, n: int, expected: str):
    assert Scheme("first_nchars", n)(value) == expected


@mark.parametrize(
    ["value", "n", "expected"],
    [
        ("abcdefg", 2, "fg"),
        ("abcdefg", 4, "defg"),
        ("abcdefg", 6, "bcdefg"),
        ("a", 2, "a"),
    ],
)
def test_last_nchars(value: str, n: int, expected: str):
    assert Scheme("last_nchars", n)(value) == expected


@mark.parametrize(
    ["value", "n", "expected"],
    [
        ("abcdefg", 4, {"abcd", "bcde", "cdef", "defg"}),
        ("abcdefg", 6, {"abcdef", "bcdefg"}),
        ("abcdefg", 8, set()),
    ],
)
def test_ngrams(value: str, n: int, expected: str):
    assert Scheme("find_ngrams", n)(value) == expected


@mark.parametrize(
    ["value", "expected"],
    [
        ("ab c de fg", "acdf"),
        ("abcdefg", "a"),
        ("abcdef g", "ag"),
    ],
)
def test_acronym(value: str, expected: str):
    assert Scheme("acronym")(value) == expected


@mark.parametrize(
    ["value", "expected"],
    [
        ("ab c de fg", "ab c de fg"),
        ("abcdefg", "abcdefg"),
        ("abcdef g", "abcdef g"),
    ],
)
def test_exactmatch(value: str, expected: str):
    assert Scheme("exactmatch")(value) == expected
