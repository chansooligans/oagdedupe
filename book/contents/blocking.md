# Blocking:

For each Union:
    For each Intersection:
        For each Pair:
            Get Pair Blocks
            Group Pair Block by Block ID and Aggregate Record IDs
        Combine Pair Blocks by getting unique combinations of Block IDs and sets of Record IDs



## Example (Dedupe):

Data:
| rec_id | name | addr |
|--|--|--|
| 0 | jason smith | 123 apple st |
| 1 | sam joe | 222 liberty |
| 2 | jason smith | 123 apple st |

Block Config:
```
Union(
            intersections = [
                    Intersection([
                        Pair(BlockAlgo=FirstLetter(),attribute="name"),
                        Pair(BlockAlgo=FirstLetter(),attribute="addr")
                    ]),
                    Intersection([
                        Pair(BlockAlgo=FirstLetter(),attribute="name"),
                        Pair(BlockAlgo=FirstLetterLastToken(),attribute="name"),
                    ])
                ]
        )
```

#### Intersection Block 1
Pair Block (BlockAlgo=FirstLetter, attribute="name"):
```
{
    "j":[0,2],
    "s":[1]
}
```

Pair Block (BlockAlgo=FirstLetter, attribute="addr"):
```
{
    "1":[0,2],
    "2":[1]
}
```

Intersection Block 1:
```
{
    ("j","1"):(0,2),
    ("j","2"):None,
    ("s","1"):None,
    ("s","2"):(1)
}
```

#### Intersection Block 2
Pair Block (BlockAlgo=FirstLetter, attribute="name"):
```
{
    "j":[0,2],
    "s":[1]
}
```

Pair Block (BlockAlgo=FirstLetterLastToken, attribute="name"):
```
{
    "s":[0,2],
    "j":[1]
}
```

Intersection Block 2:
```
{
    ("j","s"):(0,2),
    ("j","j"):None,
    ("s","s"):None,
    ("s","j"):(1)
}
```

#### Union:

{
    ("j","1"):(0,2),
    ("j","2"):None,
    ("s","1"):None,
    ("s","2"):(1)
    ("j","s"):(0,2),
    ("j","j"):None,
    ("s","s"):None,
    ("s","j"):(1)
}

#### Candidates (Dedupe):

[
    (0,2)
]