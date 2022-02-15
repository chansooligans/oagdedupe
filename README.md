# oagdedupe  

## quickstart

#### fake datasets for testing:

```
from oagdedupe.datasets.fake import df, df2
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
from oagdedupe.api import Dedupe
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
|  5 | Elizabeth Woods    | 09471 Erika Curve North Megan, UT 71358                  |    2 |         2 |
|  4 | Elizabeth Woodsx   | 09471 Erika Curve North Megan, UT 71358x                 |   12 |         2 |
|  6 | Susan Wagner       | 339 Riley Mission Suite 515 South Brendamouth, ID 32356  |    3 |         3 |
|  7 | Susan Wagnerx      | 339 Riley Mission Suite 515 South Brendamouth, ID 32356x |   13 |         3 |
|  8 | Peter Montgomery   | 35256 Craig Drive Apt. 098 North Davidborough, OK 16189  |    4 |         4 |
|  9 | Peter Montgomeryx  | 35256 Craig Drive Apt. 098 North Davidborough, OK 16189x |   14 |         4 |
| 11 | Theodore Mcgrath   | 09032 Timothy Stream Apt. 086 Port Jordanbury, SD 39961  |    5 |         5 |
| 10 | Theodore Mcgrathx  | 09032 Timothy Stream Apt. 086 Port Jordanbury, SD 39961x |   15 |         5 |
| 13 | Brian Hamiltonx    | PSC 5642, Box 8071 APO AA 97365x                         |   18 |         6 |
| 12 | Brian Hamilton     | PSC 5642, Box 8071 APO AA 97365                          |    8 |         6 |
| 14 | Susan Levyx        | 37594 Garza Roads Apt. 466 North Brookemouth, IA 29742x  |   19 |         7 |
| 15 | Susan Levy         | 37594 Garza Roads Apt. 466 North Brookemouth, IA 29742   |    9 |         7 |
| 16 | Stephanie Collins  | 87091 Gonzalez Knolls Masseyshire, UT 88665              |    6 |         8 |
| 17 | Stephanie Collinsx | 87091 Gonzalez Knolls Masseyshire, UT 88665x             |   16 |         8 |
| 18 | Stephanie Suttonx  | 225 Wilson Mills North Thomas, OK 67164x                 |   17 |         9 |
| 19 | Stephanie Sutton   | 225 Wilson Mills North Thomas, OK 67164                  |    7 |         9 |
| 20 | Susan Levy         | 37594 Garza Roads Apt. 466 North Brookemouth, IA 29742   |    9 |        10 |
| 21 | Susan Levyx        | 37594 Garza Roads Apt. 466 North Brookemouth, IA 29742x  |   19 |        10 |



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

|    | name_x          | addr_x                                            |   id_x |   cluster | name_y           | addr_y                                            |   id_y |
|---:|:----------------|:--------------------------------------------------|-------:|----------:|:-----------------|:--------------------------------------------------|-------:|
|  0 | Norma Fisher    | 80160 Clayton Isle Apt. 513 East Linda, ND 59217  |      0 |         0 | Norma Fisher     | 80160 Clayton Isle Apt. 513 East Linda, ND 59217  |      0 |
|  1 | Norma Fisher    | 80160 Clayton Isle Apt. 513 East Linda, ND 59217  |      0 |         0 | Norma Fisherx    | 80160 Clayton Isle Apt. 513 East Linda, ND 59217x |     10 |
|  2 | Norma Fisherx   | 80160 Clayton Isle Apt. 513 East Linda, ND 59217x |     10 |         0 | Norma Fisher     | 80160 Clayton Isle Apt. 513 East Linda, ND 59217  |      0 |
|  3 | Norma Fisherx   | 80160 Clayton Isle Apt. 513 East Linda, ND 59217x |     10 |         0 | Norma Fisherx    | 80160 Clayton Isle Apt. 513 East Linda, ND 59217x |     10 |
|  4 | Jorge Sullivan  | 714 Mann Plaza Suite 839 Seanfurt, OK 32234       |      1 |         1 | Jorge Sullivan   | 714 Mann Plaza Suite 839 Seanfurt, OK 32234       |      1 |
|  5 | Jorge Sullivan  | 714 Mann Plaza Suite 839 Seanfurt, OK 32234       |      1 |         1 | Jorge Sullivanx  | 714 Mann Plaza Suite 839 Seanfurt, OK 32234x      |     11 |
|  6 | Jorge Sullivanx | 714 Mann Plaza Suite 839 Seanfurt, OK 32234x      |     11 |         1 | Jorge Sullivan   | 714 Mann Plaza Suite 839 Seanfurt, OK 32234       |      1 |
|  7 | Jorge Sullivanx | 714 Mann Plaza Suite 839 Seanfurt, OK 32234x      |     11 |         1 | Jorge Sullivanx  | 714 Mann Plaza Suite 839 Seanfurt, OK 32234x      |     11 |
|  8 | Elizabeth Woods | 09471 Erika Curve North Megan, UT 71358           |      2 |         2 | Elizabeth Woods  | 09471 Erika Curve North Megan, UT 71358           |      2 |
|  9 | Elizabeth Woods | 09471 Erika Curve North Megan, UT 71358           |      2 |         2 | Elizabeth Woodsx | 09471 Erika Curve North Megan, UT 71358x          |     12 |