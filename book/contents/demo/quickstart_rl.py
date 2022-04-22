# %% [markdown]
"""
# quickstart: record linkage

The code below demonstrates how this package can be used to link two datasets. 
"""

# %% tags=["hide-cell"]
from IPython import get_ipython
if get_ipython() is not None:
    get_ipython().run_line_magic('load_ext', 'autoreload')
    get_ipython().run_line_magic('autoreload', '2')


# %%
from dedupe import base as b
from dedupe.naiveblocking import naiveblocking as n
from dedupe.blocking import blockmethod as bm
import dedupe.blocking as bl
import string
import pandas as pd
import numpy as np
from faker import Faker

# %% [markdown]
"""
## Sample Dataset to Dedupe

df contains 1000 records  
make two copies: df1 and df2 with different ruid  
(unique IDs across dataframes are required for legacy algorithm)
"""

# %%
fake = Faker()
df = pd.DataFrame({
    'ruid':np.arange(0,1000),
    'cuid':np.arange(0,1000),
    'name':[fake.name() for x in range(1000)],
    'addr':[fake.address() for x in range(1000)]
})

df1 = df.assign(ruid=lambda x:x['ruid'].astype(str)+'a')
df2 = df.assign(ruid=lambda x:x['ruid'].astype(str)+'b')

print(df1.head(3))
print(df2.head(3))

# %% [markdown]
"""
## To link records: 

(1) create a Records object using dataframes df, df2 and record_id If testing, 
 use true_id to pass the correct labels

`records = b.Records(df, df2, rec_id = 'ruid', true_id = None)`

(2) pass blocking methods

```
nb = n.NaiveBlocking(threshold=0.75, block_union = block_union)
```

where `block_union` is an object defining your blocking methods:

```
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
```

(3) run algorithm on your data

`pred = nb(records = records, cols=['name','addr'])`
"""

# %% tags=["remove-output"]
block_union = bl.Union(
    [
        bl.Intersection(
            [
                bm.Pair(method=bm.commonFourGram, attribute='name'), 
                bm.Pair(method=bm.first_letter, attribute='name')
            ]),
        bl.Intersection(
            [
                bm.Pair(method=bm.oneGramFingerprint, attribute='addr')
            ]
        )
    ],    
)


records = b.Records(df=df1, df2=df2, rec_id = 'ruid', true_id = 'cuid')
nb = b.AlgoWBlocking(threshold=0.75, block_union = block_union)
pred = nb(records = records, cols=['name','addr'])

# %% [markdown]
"""
the output `pred` contains a list of size 2 containing:
    1. the first element contains cluster IDs for df1  
    2. the second element contains cluster IDs for df2  

records that are matched share the same cluster ID
"""

# %%

df1['cluster'] = pred[0].values
df2['cluster'] = pred[1].values

df1.merge(df2, on ='cluster')
# %%
