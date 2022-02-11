# %% [markdown]
"""
# quickstart: dedupe

The code below demonstrates how this package can be used to dedupe a dataset.
"""

# %% tags=['hide-cell']
from IPython import get_ipython
if get_ipython() is not None:
    get_ipython().run_line_magic('load_ext', 'autoreload')
    get_ipython().run_line_magic('autoreload', '2')


# %%
import oaglib as oag
from oagdedupe import base as b
from oagdedupe.naiveblocking import naiveblocking as n
from oagdedupe.blocking import blockmethod as bm
import oagdedupe.blocking as bl
import string
import pandas as pd
import numpy as np
from faker import Faker

# %% [markdown]
"""
## Sample Dataset to Dedupe

df contains 1000 records, which are 500 pairs that exactly match on `name` and `addr`
"""

# %%
import string
import pandas as pd
import numpy as np
from faker import Faker
fake = Faker()
df = pd.DataFrame({
    'ruid':np.arange(0,1000),
    'cuid':np.repeat(np.arange(0,500),2),
    'name':np.repeat([fake.name() for x in range(500)],2),
    'addr':np.repeat([fake.address() for x in range(500)],2)
})
df.head(10)

# %% [markdown]
"""
## To de-dupe: 

(1) create a Records object using dataframe, record_id, and true_id if available (e.g. true_id may be used for testing benchmark dataset).  

`records = b.Records(df, rec_id = 'ruid', true_id = None)`

(2) select algorithm with the required parameters, e.g.:

`nb = n.NaiveBlocking(threshold=0.75, blocking_method = "first_letter")`

(3) run algorithm on your data

`pred = nb(records = records, cols=['name','addr'])`

"""


# %% [markdown]
"""
## Naive Blocking
"""

# %% tags=["remove-output"]
block_union = bl.union.Union(
    [
        bl.intersection.Intersection(
            [
                bm.Pair(method=bm.commonFourGram, attribute='name'), 
                bm.Pair(method=bm.first_letter, attribute='name')
            ]),
        bl.intersection.Intersection(
            [
                bm.Pair(method=bm.oneGramFingerprint, attribute='addr')
            ]
        )
    ],    
)


# %% tags=['remove-output']
records = b.Records(df=df, rec_id = 'ruid', true_id = None)
nb = n.NaiveBlocking(threshold=0.75, block_union = block_union)
pred = nb(records = records, cols=['name','addr'])

# %% [markdown]
"""
the output `pred` contains a series of length len(df) containing cluster IDs  
records that are matched share the same cluster ID
"""

# %%
df['pred'] = pred

# %%
df.sort_values('pred')