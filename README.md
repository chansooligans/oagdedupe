# dedupe  

## Run with Docker

```
# First, clone the repo
1. git clone https://github.com/chansooligans/deduper.git
2. cd deduper

# Build Docker image
3. docker build -t deduper:latest .

# Run
4. docker run -t -d --rm --name deduper -p 8080:8081 deduper 
5. Go to http://127.0.0.1:8080/load

(test dataset will be pre-loaded)
```

## to-do:

- app / active learning
    - general
        - [x] handle different column names
    - learn
        - [x] labelled samples dataframe interactive table
        - [x] previous / next / edit options for learning samples
        - [x] reformat active learning sample cards to be text instead of json
        - [x] show which dataset is being used    
        - [x] link to cards from dataframe
            - [x] ability to pass parameter to learn/<idl>-<idr> route
        - [x] if previous submit is revised, update json
        - [x] re-implenet active-dict to not relearn from already-learned samples
        - [ ] prettier presentation of counter in learn page
        - [ ] how are label="unknown" getting handled?
    - datasets
        - [x] show cached files on load page
        - [x] if no file is selected, use first in glob
    - plots
        - [ ] ability to select features
        - [x] axis labels
    - data 
        - [ ] handling multiple csv files
        - [x] select previously uploaded file
    - cached labelled samples
        - [x] clear cache option
        - [ ] upload labels option
        - [ ] download labels option
    - bugs
        - [x] retrain requires at least one sample in each class
        - [x] with each hard refresh, cached samples is popped
        - [x] redirect to load page if dataset is not loaded
        - [x] if user attempts to go to different page when dataset is not loaded, show warning
    - dockerize
        - [x] create dockerfile / .dockerignore
- algos
    - [ ] add more blocking algos
    - [ ] add other ML algos
- sql
    - [ ] mysql database
- parallelize (ray / dask)
    - [ ] parallelize blocks? 
- output
    - [x] predictions page
    - [x] download option
- record linkage option

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
