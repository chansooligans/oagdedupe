
# Terms:

- Blocking
    - Block Union: a union of Block Intersections
    - Block Intersection: an intersection of Block Methods
    - Block Method: a Pair consisting of a method and column name
    - Block Map: a dictionary where keys are cartesian product of the block IDs
    for each blocking method and values are record IDs; as an example, if the 
    block is first letter of "address" column and first letter of "name" column, then 
    one key may be "A-B" for records where "name" begins with "A" AND "address" begins with "B" 
    and the value would be a list of record IDs
    - Block ID: the keys of a block map (e.g. if a block key is "A-B" for records where "name" begins with "A" 
    and "address" begins with "B", "A-B" is a block ID)
- Candidate: a candidate pair of records that may be the same true entity
- Cluster ID: a unique identifier for same true entity
