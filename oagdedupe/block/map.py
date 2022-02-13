from tqdm import tqdm
from multiprocessing import Pool
from functools import partial
from oagdedupe import util as bu
from oagdedupe.blocking import blockmethod as bm, intersection as bi
import logging
import itertools

def check_empty(x):
    return (len(x)==0) or (list(x)[0]=='')

def get_block_map(df, rec_id: str, intersection: bi.Intersection):
    """given dataframe and block intersection, get blocks for each method-attribute pair in the intersection

    Parameters:
    ----------
    df : pd.DataFrame
        generate blocks using this dataframe
    intersection : bi.Intersection

    Returns:
    Dict -- dictionary where keys are cartesian product of the block IDs 
    for each blocking method and values are the record IDs;
    as an example, suppose blockingmethodspecs contains two blocking methods: 
    first letter of "address" column and first letter of "name" column
    then the joint_map dictionary may contain the key "A-B" whose values 
    would be the record IDs where "name" begins with "A" AND "address" begins with "B".
    """

    logging.info(f'getting blocking maps')

    # fastest way to loop through dataframe
    values = {}
    for pair in intersection.pairs:
        if pair.attribute not in values.keys():
            values[pair.attribute] = df[pair.attribute].values
    ids = df[rec_id].values

    block_map = {}

    for i in tqdm(range(df.shape[0])):
        
        block_method_keys = [
            pair.method(values[pair.attribute][i])
            for pair in intersection.pairs
        ]

        # if all methods in the pair are empty strings, skip
        check_nulls = sum([check_empty(x) for x in block_method_keys])
        if check_nulls == len(intersection.pairs):
            continue

        # cartesian product of block ids
        block_id_prod = bu.product(block_method_keys)

        block_ids = ['-'.join(block_id) for block_id in block_id_prod]

        for block_id in block_ids:
            if block_id in block_map.keys():
                block_map[block_id].append(ids[i])
            else:
                block_map[block_id] = [ids[i]]

    return block_map
