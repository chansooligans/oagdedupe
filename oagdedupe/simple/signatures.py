"""simple version signatures
"""

from .concepts import Signature
from typing import FrozenSet
from dataclasses import dataclass


@dataclass
class ValueSignature(Signature):
    value: str


@dataclass
class SetSignature(Signature):
    values: FrozenSet[str]

    def __eq__(self, other) -> bool:
        return len(self.values.intersection(other.values)) > 0
