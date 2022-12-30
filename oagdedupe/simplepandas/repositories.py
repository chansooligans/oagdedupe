"""simple version repository implementations
"""
from .concepts import LabelRepository, Label
from pandera.typing import DataFrame
from pandas import concat
from pandera import check_types


class InMemoryLabelRepository(LabelRepository):
    def __init__(self):
        self.labels = Label.example(size=0)

    @check_types
    def add(self, labels: DataFrame[Label]) -> None:
        self.labels = concat([self.labels, labels]).drop_duplicates()

    @check_types
    def get(self) -> DataFrame[Label]:
        return self.labels
