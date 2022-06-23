from dedupe.base import BaseBlockAlgo


class FirstLetter(BaseBlockAlgo):

    def get_block(self, field) -> str:
        return field.replace(' ', '')[:1]


class FirstTwoLetters(BaseBlockAlgo):

    def get_block(self, field) -> str:
        return field.replace(' ', '')[:2]


class FirstLetterLastToken(BaseBlockAlgo):

    def get_block(self, field) -> str:
        return field.split(' ')[-1].replace(' ', '')[:1]
