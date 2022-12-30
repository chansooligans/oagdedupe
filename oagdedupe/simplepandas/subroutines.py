"""
simple version subroutines
"""

from typing import Generator

from .concepts import Conjunction, Label, Pair, Record, Scheme, Attribute
from pandera.typing import DataFrame, Series
from pandas import concat
from itertools import islice


def get_signatures(
    records: DataFrame[Record], conjunction: Conjunction
) -> DataFrame[Record]:
    for scheme, attribute in conjunction:
        scheme.add(records, attribute)
    return records


def get_pairs_one_conjunction(
    records: DataFrame[Record], conjunction: Conjunction
) -> DataFrame[Pair]:
    signatures = get_signatures(records, conjunction)
    return signatures.merge(
        right=signatures,
        on=[
            scheme.name_field(attribute) for (scheme, attribute) in conjunction
        ],
        how="inner",
        suffixes=("1", "2"),
    )[[Pair.id1, Pair.id2]].query("id1 < id2")


def get_pairs_limit_pairs(
    records: DataFrame[Record],
    conjunctions: Generator[Conjunction, None, None],
    limit: int,
) -> DataFrame[Pair]:
    new_pairs = Pair.example(size=0)
    while len(new_pairs) < limit:
        pairs = new_pairs
        new_pairs = concat(
            [pairs, get_pairs_one_conjunction(records, next(conjunctions))]
        ).drop_duplicates()
    return pairs


def get_pairs_limit_conjunctions(
    records: DataFrame[Record],
    conjunctions: Generator[Conjunction, None, None],
    limit: int,
) -> DataFrame[Pair]:
    return concat(
        [
            get_pairs_one_conjunction(records, conjunction)
            for conjunction in islice(conjunctions, limit)
        ]
    ).drop_duplicates()


def get_initial_labels(records: DataFrame[Record]) -> DataFrame[Label]:
    pos = records[Record.id].sample(4, random_state=1234).values
    neg = records[Record.id].sample(4, random_state=5678).values
    return DataFrame(
        [{Label.id1: id, Label.id2: id, Label.label: True} for id in pos]
        + [
            {Label.id1: id1, Label.id2: id2, Label.label: False}
            for id1 in neg
            for id2 in neg
            if id1 < id2
        ]
    )
