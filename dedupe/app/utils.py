import json
from collections import Counter
from functools import cached_property

class Labels:

    def __init__(self, cache_path):
        self.cache_path = cache_path

    @cached_property
    def labels(self):
        with open(f"{self.cache_path}/samples.json", "r") as f:
            return json.load(f)

    @property
    def label_counts(self):
        label_counts = Counter()
        for val in self.labels.values():
            label_counts[val["label"]] += 1
        return label_counts

    @property
    def sufficient_positive_negative(self):
        return (self.label_counts["Yes"] > 5) & (self.label_counts["No"] > 5) 

    @property
    def _type(self):
        if self.sufficient_positive_negative:
            return "uncertain"
        else:
            return "init"

    @property
    def sampleidx(self):
        print(len(self.labels))
        return len(self.labels)

    def save(self):
        with open(f"{self.cache_path}/samples.json", "w") as f:
            json.dump(self.labels, f)
        del self.labels
