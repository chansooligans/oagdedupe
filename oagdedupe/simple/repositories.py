"""simple version repository implementations
"""
from .concepts import LabelRepository, Pair, Label, ClassifierRepository, Classifier
from typing import Dict, Optional


class InMemoryLabelRepository(LabelRepository):
    def __init__(self):
        self.labels = {}

    def add(self, pair: Pair, label: Label) -> None:
        self.labels[pair] = label

    def get(self) -> Dict[Pair, Label]:
        return self.labels


class InMemoryClassifierRepository(ClassifierRepository):
    def __init__(self):
        self.classifier: Optional[Classifier] = None

    def add(self, classifier: Classifier) -> None:
        self.classifier = classifier

    def get(self) -> Optional[Classifier]:
        return self.classifier
