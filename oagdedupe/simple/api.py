"""
top-level API for this simple dedupe version
"""

from .concepts import Record, Entity
from typing import Set


def dedupe(records: Set[Record]) -> Set[Entity]:
    pass