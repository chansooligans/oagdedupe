import json
from collections import defaultdict
from functools import cached_property
from io import BytesIO
import base64
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(rc={'figure.figsize':(11.7,8.27)})


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

def get_plots(dfX, scores):

    img = BytesIO()
    plt.figure()
    sns.scatterplot(x=0, y=1, hue = "scores", data=dfX)
    plt.savefig(img, format='png')
    plt.close()
    
    img2 = BytesIO()
    plt.figure()
    sns.kdeplot(dfX["scores"])
    sns.histplot(dfX["scores"])
    plt.savefig(img2, format='png')
    plt.close()

    img.seek(0)
    scatterplt = base64.b64encode(img.getvalue()).decode('utf8')

    img2.seek(0)
    kdeplot = base64.b64encode(img2.getvalue()).decode('utf8')

    return scatterplt, kdeplot