# %%
from IPython import get_ipython
if get_ipython() is not None:
    get_ipython().run_line_magic('load_ext', 'autoreload')
    get_ipython().run_line_magic('autoreload', '2')
from dedupe.api import Dedupe
from dedupe.block import blockers 
from dedupe.block import algos
from dedupe.datasets.fake import df
from dedupe import config

manual_blocker = blockers.ManualBlocker([
    [(algos.FirstNLetters(N=1), "name"), (algos.FirstNLetters(N=1), "addr")],
    [(algos.FirstNLettersLastToken(N=1), "name"), (algos.FirstNLetters(N=1), "name")],
])

attributes = ["name", "addr"]

d = Dedupe(
    df=df, 
    attributes=attributes,
    blocker=manual_blocker
)

# d.train()

# %%
preds = d.predict()

# %%
preds["cluster"].value_counts()
# %%
df.merge(
    preds,
    left_index=True,
    right_on="id"
).sort_values('cluster').query("cluster==1")

# %%
