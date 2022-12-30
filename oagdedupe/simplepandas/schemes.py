"""
simple version schemes
"""
from pandera.typing import Series
from .concepts import Scheme


class FirstLetterFirstWord(Scheme):
    def get(value: str) -> str:
        return value.split(" ")[0][0]
