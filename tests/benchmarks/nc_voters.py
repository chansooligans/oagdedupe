# %%
from IPython import get_ipython
if get_ipython() is not None:
    get_ipython().run_line_magic('load_ext', 'autoreload')
    get_ipython().run_line_magic('autoreload', '2')
import glob
import pandas as pd
files = glob.glob('/mnt/Research.CF/References & Training/Satchel/dedupe_rl/baseline_datasets/north_carolina_voters/*')

df = pd.concat([
    pd.read_csv(f)
    for f in files
]).reset_index(drop=True)

attributes = ["givenname","surname","suburb","postcode"]
for attr in attributes:
    df[attr] = df[attr].astype(str)

# %%
from dedupe.api import Dedupe
from dedupe.distance.string import RayAllJaro
from dedupe.block import blockers 
from dedupe.block import algos
from dedupe.train.threshold import Threshold

manual_blocker = blockers.ManualBlocker([
    [
        (algos.FirstNLetters(N=3), "givenname"), 
        (algos.FirstNLetters(N=3), "surname"), 
        (algos.ExactMatch(), "suburb"), 
    ],
    [
        (algos.FirstNLetters(N=3), "givenname"), 
        (algos.FirstNLetters(N=3), "surname"), 
        (algos.ExactMatch(), "postcode"), 
    ],
])

d = Dedupe(
    df=df, 
    attributes=attributes, 
    blocker=manual_blocker,
    trainer=Threshold(threshold=0.95),
    distance=RayAllJaro(), 
    cpus=20
)
preds = d.predict()

# %%
df.merge(preds, left_index=True, right_on="id").sort_values("cluster").head(20)


# %%
