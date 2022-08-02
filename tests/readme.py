# %%
from IPython import get_ipython
if get_ipython() is not None:
    get_ipython().run_line_magic('load_ext', 'autoreload')
    get_ipython().run_line_magic('autoreload', '2')
from dedupe.datasets.fake import df, df2
print(df.head())


# %% [markdown]
"""
# Dedupe 
"""

# %%
from dedupe.api import Dedupe
d = Dedupe(df=df, attributes=None)
preds = d.predict()

df.merge(preds, left_index=True, right_on="id").sort_values("cluster")

# %% [markdown]
"""
# Manual Blocker
"""
from dedupe.api import Dedupe
from dedupe.block import blockers 
from dedupe.block import algos

manual_blocker = blockers.ManualBlocker([
    [(algos.FirstNLetters(N=1), "name"), (algos.FirstNLetters(N=1), "addr")],
    [(algos.FirstNLettersLastToken(N=1), "name"), (algos.FirstNLetters(N=1), "name")],
])

d = Dedupe(df=df, attributes=None, blocker=manual_blocker)
preds = d.predict()

df.merge(preds, left_index=True, right_on="id").sort_values("cluster")


# %% [markdown]
"""
# Parallel
"""

# %%
from dedupe.api import Dedupe
from dedupe.distance.string import RayAllJaro
d = Dedupe(df=df, attributes=None, distance=RayAllJaro(), cpus=20)
preds = d.predict()

df.merge(preds, left_index=True, right_on="id").sort_values("cluster")

