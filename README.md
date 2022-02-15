# oagdedupe  

## quickstart

#### fake datasets for testing:

```
from oagdedupe.datasets.fake import df, df2
```

#### dedupe:

to specify columns to dedupe on, set `attributes = ["name", "addr"]`

```
d = Dedupe(df=df, attributes=None)
preds = d.predict()

df.merge(preds, left_index=True, right_on="id")
```

#### record linkage:

to specify columns to dedupe on, set `attributes = ["name", "addr"]`

```
rl = RecordLinkage(df=df, df2=df2, attributes=attributes, attributes2=attributes)
predsx, predsy = rl.predict()

pd.merge(
    df.merge(predsx, left_index=True, right_on="id"),
    df2.merge(predsy, left_index=True, right_on="id"),
    on="cluster",
)
```
