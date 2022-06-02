# %%
from IPython import get_ipython
if get_ipython() is not None:
    get_ipython().run_line_magic('load_ext', 'autoreload')
    get_ipython().run_line_magic('autoreload', '2')
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(rc={'figure.figsize':(11.7,8.27)})
import streamlit as st
import streamlit.components.v1 as components

from dedupe.api import Dedupe, RecordLinkage
from dedupe.train.active import Active
from dedupe.datasets.fake import df, df2

import pandas as pd
import numpy as np

# %%
d = Dedupe(df=df, trainer=Active())
idxmat = d._get_candidates()
X = d.distance.get_distmat(d.df, d.df2, d.attributes, d.attributes2, idxmat)
d.trainer.initialize(X)

# %%
d.trainer.get_samples()

# %%
_type = "init"

samples = pd.concat(
    [
        df.loc[idxmat[d.trainer.samples[_type],0]].reset_index(drop=True),
        df.loc[idxmat[d.trainer.samples[_type],1]].reset_index(drop=True)
    ],
    axis=1
).assign(
    score=d.trainer.scores[d.trainer.samples[_type]],
    label=None
)
samples

# %%
samples["label"] = [0,0,0,0,1,1,1,1,1,1]

d.trainer.labels[_type] = samples["label"].values

for idx,lab in zip(d.trainer.samples[_type], d.trainer.labels[_type]):
    d.trainer.active_dict[idx] = lab

d.trainer.train(
    X[list(d.trainer.active_dict.keys()),:], 
    init=False, 
    labels=list(d.trainer.active_dict.values())
)
d.trainer.scores, d.trainer.y = d.trainer.fit(X)


# %%
_type = "uncertain"

samples = pd.concat(
    [
        df.loc[idxmat[d.trainer.samples[_type],0]].reset_index(drop=True),
        df.loc[idxmat[d.trainer.samples[_type],1]].reset_index(drop=True)
    ],
    axis=1
).assign(
    score=d.trainer.scores[d.trainer.samples[_type]],
    label=None
)
samples

# %%
samples["label"] = [1,1,1,1,1]

d.trainer.labels[_type] = samples["label"].values

for idx,lab in zip(d.trainer.samples[_type], d.trainer.labels[_type]):
    d.trainer.active_dict[idx] = lab

d.trainer.train(
    X[list(d.trainer.active_dict.keys()),:], 
    init=False, 
    labels=list(d.trainer.active_dict.values())
)
d.trainer.scores, d.trainer.y = d.trainer.fit(X)

# %%
