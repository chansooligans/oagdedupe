"""
simple version schemes
"""

from .concepts import Scheme, Record, Attribute
from typing import Tuple


class FirstLetterFirstWord(Scheme):
    def get_signature(record: Record, attribute: Attribute):
        return record.values[attribute].split(" ")[0][0]

    def signatures_match(sigs: Tuple) -> bool:
        return len(set(sigs)) == 1
