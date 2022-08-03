# %%
from IPython import get_ipython
if get_ipython() is not None:
    get_ipython().run_line_magic('load_ext', 'autoreload')
    get_ipython().run_line_magic('autoreload', '2')
from dedupe.api import Dedupe
from dedupe.block import blockers 
from dedupe.block import algos
from dedupe.datasets.fake import df

manual_blocker = blockers.ManualBlocker([
    [(algos.FirstNLetters(N=1), "name"), (algos.FirstNLetters(N=1), "addr")],
    [(algos.FirstNLettersLastToken(N=1), "name"), (algos.FirstNLetters(N=1), "name")],
])

attributes = ["name", "addr"]

cache_fp="../../cache/test.db"

d = Dedupe(
    df=df, 
    attributes=attributes,
    blocker=manual_blocker,
    cache_fp=cache_fp,
)

d.train()

# %%
from dedupe.fastapi import utils as u

# %%
active_model_fp = "/mnt/Research.CF/References & Training/Satchel/dedupe_rl/active_models/test_df.pkl" 
cache_fp = "/home/csong/cs_github/deduper/cache/test.db"
model = u.Model(cache_fp=cache_fp, active_model_fp=active_model_fp)
# %%
model.X
# %%
import pandas as pd
pd.read_sql("SELECT 1",con=model.engine)
# %%
