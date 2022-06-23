# %%
from dedupe.block import algos
import pytest


@pytest.fixture()
def names():
    return ["John1 Jacob", "Gill2 Foo"]


def test_FirstNLetters(names):
    blocks = [
        algos.FirstNLetters(N=1).get_block(f)
        for f in names
    ]
    assert blocks == ["J", "G"]


def test_FirstNLettersLastToken(names):
    blocks = [
        algos.FirstNLettersLastToken(N=1).get_block(f)
        for f in names
    ]
    assert blocks == ["J", "F"]


def test_LastNLetters(names):
    blocks = [
        algos.LastNLetters(N=1).get_block(f)
        for f in names
    ]
    assert blocks == ["b", "o"]


def test_NumbersOnly(names):
    blocks = [
        algos.NumbersOnly().get_block(f)
        for f in names
    ]
    assert blocks == ["1", "2"]


def test_ExactMatch(names):
    blocks = [
        algos.ExactMatch().get_block(f)
        for f in names
    ]
    assert blocks == ["John1 Jacob", "Gill2 Foo"]