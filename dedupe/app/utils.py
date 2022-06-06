import json
from collections import defaultdict
from functools import cached_property

class Labels:

    def __init__(self, cache_path):
        self.cache_path = cache_path

    @cached_property
    def labels(self):
        with open(f"{self.cache_path}/samples.json", "r") as f:
            return json.load(f)

    @cached_property
    def meta(self):
        with open(f"{self.cache_path}/meta.json", "r") as f:
            meta = json.load(f)
        return defaultdict(int, meta)

    @property
    def sufficient_positive_negative(self):
        return (self.meta["Yes"] >= 5) & (self.meta["No"] >= 5) 

    @property
    def _type(self):
        if self.sufficient_positive_negative:
            return "uncertain"
        else:
            return "init"

    @property
    def sampleidx(self):
        return self.meta[self._type+"_current"]

    def save(self):
        with open(f"{self.cache_path}/samples.json", "w") as f:
            json.dump(self.labels, f)
        with open(f"{self.cache_path}/meta.json", "w") as f:
            json.dump(self.meta, f)
        del self.labels, self.meta
