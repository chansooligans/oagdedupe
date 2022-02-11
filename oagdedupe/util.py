import numpy as np
import pandas as pd
import networkx as nx
from typing import List,Dict

def subset_chars(col, slicer):
    return col[slicer]

def normalize(x):
    return (x-min(x))/(max(x)-min(x))

def get_weights(g, clusters):
    
    weights = []
    subgraphs = [g.subgraph(c) for c in clusters]
    for cluster_idx,subgraph in enumerate(subgraphs):
        weightlist = [x[2]['weight'] for x in subgraph.edges().data()]

        weights_dict = {
            'cluster_id':cluster_idx,
            'csize':len(weightlist),
            'cmin':min(weightlist),
            'cmax':max(weightlist),
            'cmean':np.mean(weightlist)
        }
        
        weights.append(weights_dict)
    return pd.DataFrame(weights)

def get_connectivity_stats(matches, clusters, simcol='similarity'):

    long = [
        (
            matches[['index_'+char, simcol]]
            .rename({'index_'+char:'index'},axis=1) 
        )
        for char in ['A','B']
    ]

    connectivity = (
        pd.concat(long)
        .groupby('index')
        .agg({simcol:[len, np.min, np.max, np.mean]})
        .reset_index()
    )

    connectivity.columns = ['index', 'elen', 'emin', 'emax', 'emean']

    # Assign clusters and export
    for cluster_idx,cluster in enumerate(clusters):
        connectivity.loc[connectivity['index'].isin(cluster), 'ecluster_id'] = cluster_idx

    return connectivity


def comb_fields(df: pd.DataFrame, cols: List[str]) -> pd.Series:
    space = df[cols[0]].map(lambda x: " ")
    comb = df[cols[0]].astype(str)
    for col in cols[1:]:
        comb = comb + space + df[col].astype(str)
    return comb

def product(lists, repeat=1, nodupes=True):
    # copied from itertools package and added nodupes option
    # cartesian product of lists
    pools = [tuple(pool) for pool in lists] * repeat
    result = [[]]
    for pool in pools:
        if nodupes==True:
            result = [x+[y] for x in result for y in pool if x != [y]]
        else:
            result = [x+[y] for x in result for y in pool]
    return result

def intersection(list_of_lists):
    return set.intersection(*map(set,list_of_lists))