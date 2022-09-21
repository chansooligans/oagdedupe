from dedupe.settings import Settings
from dedupe.db.tables import Tables

from functools import cached_property
from sqlalchemy import create_engine
from sqlalchemy.orm import aliased
from dataclasses import dataclass
import pandas as pd

def check_unnest(name):
    if "ngrams" in name:
        return f"unnest({name})"
    return name

def signatures(names):
    return ", ".join([
        f"{check_unnest(name)} as signature{i}" 
        for i,name in  enumerate(names)
    ])

@dataclass
class DatabaseCore:
    settings: Settings

    def __post_init__(self):
        self.engine_url = self.settings.other.path_database
        self.schema = self.settings.other.db_schema

    def query(self, sql):
        """
        for parallel implementation, need to create separate engine 
        for each process
        """
        engine = create_engine(self.engine_url)
        res = pd.read_sql(sql, con=engine)
        engine.dispose()
        return res

    def get_labels(self):
        return self.query(
            f"""
            SELECT * FROM dedupe.labels
            """
        )

    def get_inverted_index(self, names, table):
        return self.query(
            f"""
            WITH 
                inverted_index AS (
                    SELECT 
                        {signatures(names)}, 
                        ARRAY_AGG(_index ORDER BY _index asc) as array_agg
                    FROM {self.schema}.{table}
                    GROUP BY {", ".join([f"signature{i}" for i in range(len(names))])}
                )
            SELECT * 
            FROM inverted_index
            WHERE array_length(array_agg, 1) > 1
            """
        )

    def get_inverted_index_pairs(self, names, table):
        return self.query(f"""
            WITH 
                inverted_index AS (
                    SELECT 
                        {signatures(names)}, 
                        ARRAY_AGG(_index ORDER BY _index asc) as array_agg
                    FROM {self.schema}.{table}
                    GROUP BY {", ".join([f"signature{i}" for i in range(len(names))])}
                ),
                inverted_index_subset AS (
                    SELECT * 
                    FROM inverted_index
                    WHERE array_length(array_agg, 1) > 1
                ),
                combinations AS (
                    SELECT unnest_2d_1d(combinations(array_agg)) as pairs
                    FROM inverted_index_subset
                )
            SELECT pairs[1] as _index_l, pairs[2] as _index_r, True as blocked
            FROM combinations
            """)

    @cached_property
    def blocking_schemes(self):
        """
        get all blocking schemes
        """
        return self.query(
            f"SELECT * FROM {self.schema}.blocks_train LIMIT 1"
        ).columns[1:]


@dataclass
class DatabaseORM(Tables, DatabaseCore):
    settings: Settings

    def __post_init__(self):
        self.engine_url = self.settings.other.path_database
        self.schema = self.settings.other.db_schema
        self.attributes = self.settings.other.attributes

    @cached_property
    def engine(self):
        return create_engine(self.settings.other.path_database)

    def get_train(self):
        with self.Session() as session:
            query = session.query(self.Train)
            return pd.read_sql(query.statement, query.session.bind)

    def get_distances(self):
        """
        get unlabelled distances for sample data
        """
        with self.Session() as session:
            query = (
                session
                .query(self.Distances)
                .join(
                    self.Labels, 
                    (self.Distances._index_l==self.Labels._index_l) & 
                    (self.Distances._index_r==self.Labels._index_r), 
                    isouter=True)
                .filter(self.Labels.label == None)
                )
            return pd.read_sql(query.statement, query.session.bind)

    def get_full_distances(self):
        """
        get distances for full data
        """
        with self.Session() as session:
            query = (
                session
                .query(
                    *(
                        getattr(self.FullDistances,x) 
                        for x in self.attributes
                    )
                )
                )
            return pd.read_sql(query.statement, query.session.bind)

    def get_full_comparison_indices(self):
        with self.Session() as session:
            query = (
                session
                .query(self.FullDistances._index_l, self.FullDistances._index_r)
                .order_by(self.FullDistances._index_l, self.FullDistances._index_r)
                )
            return pd.read_sql(query.statement, query.session.bind)

    def get_compare_cols(self):
        columns = [
            [f"{x}_l" for x in self.attributes], 
            ["_index_l"],
            [f"{x}_r" for x in self.attributes],
            ["_index_r"]
        ]
        return sum(columns, [])
    
    def get_comparison_attributes(self, table):

        fields_table = {
            "comparisons":(self.Comparisons,self.Sample),
            "full_comparisons":(self.FullComparisons,self.maindf)
        }
        pairs, data = fields_table[table]

        with self.Session() as session:
            dataL = aliased(data)
            dataR = aliased(data)
            query = (
                session
                .query(
                    pairs._label_key,
                    *(
                        getattr(dataL,x).label(f"{x}_l")
                        for x in self.settings.other.attributes + ["_index"]
                    ),
                    *(
                        getattr(dataR,x).label(f"{x}_r")
                        for x in self.settings.other.attributes + ["_index"]
                    ),
                )
                .outerjoin(dataL, pairs._index_l==dataL._index)
                .outerjoin(dataR, pairs._index_r==dataR._index)
                .order_by(pairs._index_l, pairs._index_r)
                )

            return (
                pd.read_sql(query.statement, query.session.bind)
                .drop(["_label_key"], axis=1)
            )

    def get_label_attributes(self):

        with self.Session() as session:
            
            dataL = session.query(self.Train).distinct(self.Train._index).subquery()
            dataR = session.query(self.Train).distinct(self.Train._index).subquery()
            
            query = (
                session
                .query(
                    self.Labels.label,
                    *(
                        getattr(dataL.c,x).label(f"{x}_l")
                        for x in self.settings.other.attributes
                    ),
                    dataL.c._index["_index"].label("_index_l"),
                    *(
                        getattr(dataR.c,x).label(f"{x}_r")
                        for x in self.settings.other.attributes
                    ),
                    dataR.c._index["_index"].label("_index_r"),
                )
                .outerjoin(dataL, self.Labels._index_l==dataL.c._index["_index"])
                .outerjoin(dataR, self.Labels._index_r==dataR.c._index["_index"])
                .order_by(self.Labels._index_l, self.Labels._index_r)
                )

            return pd.read_sql(query.statement, query.session.bind)

    def get_clusters(self):
        with self.Session() as session:
            query = (
                session
                .query(self.Clusters.cluster, self.maindf)
                .join(self.maindf, self.Clusters._index == self.maindf._index)
                .order_by(self.Clusters.cluster)
                )
            return pd.read_sql(query.statement, query.session.bind)
        