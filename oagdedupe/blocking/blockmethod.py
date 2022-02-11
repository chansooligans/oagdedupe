"""general tools for blocking methods
"""

from dataclasses import dataclass
from typing import Any, Callable, Set, Tuple, Sequence, Union
import pandas as pd
import re
from dedupe.cpredicates import ngrams, initials
from dedupe.predicates import (
    words,
    integers,
    start_word,
    start_integer,
    alpha_numeric,
    sortedAcronym,
    oneGramFingerprint,
    twoGramFingerprint,
    commonFourGram,
    commonSixGram
)

Method = Callable[[str], Union[Tuple[str], Set[str]]]


def first_letter(field: str) -> Tuple[str]:
    return initials(field.replace(' ', ''), 1)


def first_two_letters(field: str) -> Tuple[str]:
    return initials(field.replace(' ', ''), 2)


def first_four_letters(field: str) -> Tuple[str]:
    return initials(field.replace(' ', ''), 4)


def first_six_letters(field: str) -> Tuple[str]:
    return initials(field.replace(' ', ''), 6)


def first_letter_last_token(field: str) -> Tuple[str]:
    return initials(field.split(' ')[-1].replace(' ', ''), 1)


methods: Set[Method] = {
    first_letter,
    first_two_letters,
    first_four_letters,
    first_six_letters,
    first_letter_last_token,
    sortedAcronym,
    oneGramFingerprint,
    twoGramFingerprint,
    commonFourGram,
}


def get_method(name: str) -> Method:
    """get a method from the name of a method, raises exception if there are no or more than one methods with the given name

    Parameters
    ----------
    name : str
        name of method

    Returns
    -------
    Method
        method matching on name
    """
    matches = {i for i in methods if i.__name__ == name}
    assert (
        len(matches) == 1
    ), f"one match for name {name} not found: matches = {matches}"
    return matches.pop()


@dataclass(frozen=True)
class Pair:
    """method-attribute pair"""

    method: Method
    attribute: str

    def __str__(self) -> str:
        return f"{self.method.__name__}-{self.attribute}"
