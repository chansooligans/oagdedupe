# oagdedupe  

## quickstart

#### fake datasets for testing:

```
from oagdedupe.datasets.fake import df, df2
print(df.head())
```


|    | name             | addr                         |
|---:|:-----------------|:-----------------------------|
|  0 | Norma Fisher     | 80160 Clayton Isle Apt. 513  |
|    |                  | East Linda, ND 59217         |
|  1 | Jorge Sullivan   | 714 Mann Plaza Suite 839     |
|    |                  | Seanfurt, OK 32234           |
|  2 | Elizabeth Woods  | 09471 Erika Curve            |
|    |                  | North Megan, UT 71358        |
|  3 | Susan Wagner     | 339 Riley Mission Suite 515  |
|    |                  | South Brendamouth, ID 32356  |
|  4 | Peter Montgomery | 35256 Craig Drive Apt. 098   |
|    |                  | North Davidborough, OK 16189 |

#### dedupe:

```
from oagdedupe.api import Dedupe
d = Dedupe(df=df, attributes=None)
preds = d.predict()

df.merge(preds, left_index=True, right_on="id").sort_values("cluster")
```

(optional: to specify columns to dedupe on, set attributes, e.g. `attributes = ["name", "addr"]`)

|    | name            | addr                        |   id |   cluster |
|---:|:----------------|:----------------------------|-----:|----------:|
|  0 | Norma Fisher    | 80160 Clayton Isle Apt. 513 |    0 |         0 |
|    |                 | East Linda, ND 59217        |      |           |
|  1 | Norma Fisherx   | 80160 Clayton Isle Apt. 513 |   10 |         0 |
|    |                 | East Linda, ND 59217x       |      |           |
|  2 | Jorge Sullivan  | 714 Mann Plaza Suite 839    |    1 |         1 |
|    |                 | Seanfurt, OK 32234          |      |           |
|  3 | Jorge Sullivanx | 714 Mann Plaza Suite 839    |   11 |         1 |
|    |                 | Seanfurt, OK 32234x         |      |           |
|  5 | Elizabeth Woods | 09471 Erika Curve           |    2 |         2 |
|    |                 | North Megan, UT 71358       |      |           |



#### record linkage:

```
from oagdedupe.api import RecordLinkage
rl = RecordLinkage(df=df, df2=df2, attributes=None, attributes2=None)
predsx, predsy = rl.predict()

pd.merge(
    df.merge(predsx, left_index=True, right_on="id"),
    df2.merge(predsy, left_index=True, right_on="id"),
    on="cluster",
)
```

(optional: to specify columns of df2 to dedupe on, use attributes2, e.g. `attributes2 = ["name", "addr"]`)

|    | name_x          | addr_x                      |   id_x |   cluster | name_y           | addr_y                      |   id_y |
|---:|:----------------|:----------------------------|-------:|----------:|:-----------------|:----------------------------|-------:|
|  0 | Norma Fisher    | 80160 Clayton Isle Apt. 513 |      0 |         0 | Norma Fisher     | 80160 Clayton Isle Apt. 513 |      0 |
|    |                 | East Linda, ND 59217        |        |           |                  | East Linda, ND 59217        |        |
|  1 | Norma Fisher    | 80160 Clayton Isle Apt. 513 |      0 |         0 | Norma Fisherx    | 80160 Clayton Isle Apt. 513 |     10 |
|    |                 | East Linda, ND 59217        |        |           |                  | East Linda, ND 59217x       |        |
|  2 | Norma Fisherx   | 80160 Clayton Isle Apt. 513 |     10 |         0 | Norma Fisher     | 80160 Clayton Isle Apt. 513 |      0 |
|    |                 | East Linda, ND 59217x       |        |           |                  | East Linda, ND 59217        |        |
|  3 | Norma Fisherx   | 80160 Clayton Isle Apt. 513 |     10 |         0 | Norma Fisherx    | 80160 Clayton Isle Apt. 513 |     10 |
|    |                 | East Linda, ND 59217x       |        |           |                  | East Linda, ND 59217x       |        |
|  4 | Jorge Sullivan  | 714 Mann Plaza Suite 839    |      1 |         1 | Jorge Sullivan   | 714 Mann Plaza Suite 839    |      1 |
|    |                 | Seanfurt, OK 32234          |        |           |                  | Seanfurt, OK 32234          |        |
|  5 | Jorge Sullivan  | 714 Mann Plaza Suite 839    |      1 |         1 | Jorge Sullivanx  | 714 Mann Plaza Suite 839    |     11 |
|    |                 | Seanfurt, OK 32234          |        |           |                  | Seanfurt, OK 32234x         |        |
|  6 | Jorge Sullivanx | 714 Mann Plaza Suite 839    |     11 |         1 | Jorge Sullivan   | 714 Mann Plaza Suite 839    |      1 |
|    |                 | Seanfurt, OK 32234x         |        |           |                  | Seanfurt, OK 32234          |        |
|  7 | Jorge Sullivanx | 714 Mann Plaza Suite 839    |     11 |         1 | Jorge Sullivanx  | 714 Mann Plaza Suite 839    |     11 |
|    |                 | Seanfurt, OK 32234x         |        |           |                  | Seanfurt, OK 32234x         |        |
|  8 | Elizabeth Woods | 09471 Erika Curve           |      2 |         2 | Elizabeth Woods  | 09471 Erika Curve           |      2 |
|    |                 | North Megan, UT 71358       |        |           |                  | North Megan, UT 71358       |        |
|  9 | Elizabeth Woods | 09471 Erika Curve           |      2 |         2 | Elizabeth Woodsx | 09471 Erika Curve           |     12 |
|    |                 | North Megan, UT 71358       |        |           |                  | North Megan, UT 71358x      |        |