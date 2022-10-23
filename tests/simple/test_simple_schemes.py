"""
testing simple version schemes
"""

from oagdedupe.simple.schemes import first_letter_first_word
from pytest import mark

@mark.parametrize(
    "scheme,value,expected",
    [
        (first_letter_first_word, "testing this value", "t"),
        (first_letter_first_word, "123 some road", "1"),
    ],
)
def test_get_signature_works(scheme, value, expected):
    assert scheme(value) == expected


@mark.parametrize(
    "scheme,value1,value2,expected",
    [
        (first_letter_first_word, "testing test", "test, testing", True),
        (first_letter_first_word, "test test", "112 some road", False),
    ],
)
def test_signatures_match_works(scheme, value1, value2, expected):
    assert (scheme(value1) == scheme(value2)) is expected
