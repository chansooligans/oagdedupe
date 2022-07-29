from dedupe.base import BaseBlockAlgo

from dataclasses import dataclass
import re


@dataclass
class FirstNLetters(BaseBlockAlgo):
    N: int

    def get_block(self, field) -> str:
        return field.replace(' ', '')[:self.N]


@dataclass
class FirstNLettersLastToken(BaseBlockAlgo):
    N: int

    def get_block(self, field) -> str:
        return field.split(' ')[-1].replace(' ', '')[:self.N]


@dataclass
class LastNLetters(BaseBlockAlgo):
    N: int

    def get_block(self, field) -> str:
        return field.replace(' ', '')[-self.N:]


@dataclass
class NumbersOnly(BaseBlockAlgo):

    def get_block(self, field) -> str:
        return re.sub("[^0-9]", "", field)


class ExactMatch(BaseBlockAlgo):

    def get_block(self, field) -> str:
        return field
