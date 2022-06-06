# dedupe  

## to-do:

- app / active learning
    - learn
        - previous / next / edit options for learning samples
        - editable json?
    - datasets
        - show cached files on load page
- algos
    - add more blocking algos
- scale
    - sql database
    - parallelize

## quickstart

#### fake datasets for testing:

```
from dedupe.datasets.fake import df, df2
print(df.head())
```

|    | name             | addr                                                    |
|---:|:-----------------|:--------------------------------------------------------|
|  0 | Norma Fisher     | 80160 Clayton Isle Apt. 513 East Linda, ND 59217        |
|  1 | Jorge Sullivan   | 714 Mann Plaza Suite 839 Seanfurt, OK 32234             |
|  2 | Elizabeth Woods  | 09471 Erika Curve North Megan, UT 71358                 |
|  3 | Susan Wagner     | 339 Riley Mission Suite 515 South Brendamouth, ID 32356 |
|  4 | Peter Montgomery | 35256 Craig Drive Apt. 098 North Davidborough, OK 16189 |

#### dedupe:

```
from dedupe.api import Dedupe
d = Dedupe(df=df, attributes=None)
preds = d.predict()

df.merge(preds, left_index=True, right_on="id").sort_values("cluster")
```

(optional: to specify columns to dedupe on, set attributes, e.g. `attributes = ["name", "addr"]`)

|    | name               | addr                                                     |   id |   cluster |
|---:|:-------------------|:---------------------------------------------------------|-----:|----------:|
|  0 | Norma Fisher       | 80160 Clayton Isle Apt. 513 East Linda, ND 59217         |    0 |         0 |
|  1 | Norma Fisherx      | 80160 Clayton Isle Apt. 513 East Linda, ND 59217x        |   10 |         0 |
|  2 | Jorge Sullivan     | 714 Mann Plaza Suite 839 Seanfurt, OK 32234              |    1 |         1 |
|  3 | Jorge Sullivanx    | 714 Mann Plaza Suite 839 Seanfurt, OK 32234x             |   11 |         1 |


#### record linkage:

```
from dedupe.api import RecordLinkage
rl = RecordLinkage(df=df, df2=df2, attributes=None, attributes2=None)
predsx, predsy = rl.predict()

pd.merge(
    df.merge(predsx, left_index=True, right_on="id"),
    df2.merge(predsy, left_index=True, right_on="id"),
    on="cluster",
)
```

(optional: to specify columns of df2 to dedupe on, use attributes2, e.g. `attributes2 = ["name", "addr"]`)

|    | name_x          | addr_x                                            |   id_x |   cluster | name_y           | addr_y                                            |   id_y |
|---:|:----------------|:--------------------------------------------------|-------:|----------:|:-----------------|:--------------------------------------------------|-------:|
|  0 | Norma Fisher    | 80160 Clayton Isle Apt. 513 East Linda, ND 59217  |      0 |         0 | Norma Fisher     | 80160 Clayton Isle Apt. 513 East Linda, ND 59217  |      0 |
|  1 | Norma Fisher    | 80160 Clayton Isle Apt. 513 East Linda, ND 59217  |      0 |         0 | Norma Fisherx    | 80160 Clayton Isle Apt. 513 East Linda, ND 59217x |     10 |
|  2 | Norma Fisherx   | 80160 Clayton Isle Apt. 513 East Linda, ND 59217x |     10 |         0 | Norma Fisher     | 80160 Clayton Isle Apt. 513 East Linda, ND 59217  |      0 |
|  3 | Norma Fisherx   | 80160 Clayton Isle Apt. 513 East Linda, ND 59217x |     10 |         0 | Norma Fisherx    | 80160 Clayton Isle Apt. 513 East Linda, ND 59217x |     10 |
