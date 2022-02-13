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