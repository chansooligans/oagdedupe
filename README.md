# dedupe  

- new draft of dedupe tool i helped develop with the research and analytics departement of the ny state office of the attorney general
- partly a learning project and hopefully an easy-to-use, powerful entity resolution tool for general public to use

## quickstart

#### install dependencies with poetry:

install poetry if needed: https://python-poetry.org/docs/#installation

```
poetry install
```

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
import pandas as pd
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


#### manual blocking specifications:

Suppose you want to specificy your own blocking schemes. And you want to use 
two "intersections" of blocking algos: 
    - (1) compare all instances where records share first letter of name AND 
    first letter of address
    - (2) compare all instances where records share first letter of the last 
    token of name AND first letter of name

Each intersection may contain at least one blocking algo.   

The goal is to identify the intersections that yields the most true positives, 
while minimizing the # of possible comparisons.  

See `dedupe.block.algos` for all blocking algo options.

```
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
```

#### active learning

```
# streamlit for labelling
streamlit run app/app.py --server.port 8089

# model
from dedupe.api import Dedupe
from dedupe.train.active import Active
d = Dedupe(
    df=df, 
    trainer=Active(cache_fp="../cache/test.csv"),
)
preds = d.predict()
```