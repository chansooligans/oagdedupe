from dedupe.settings import Settings
from dedupe.db.tables import Tables

from functools import cached_property
import sqlalchemy
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
    """
    Object contains methods to query database using sqlalchemy CORE;
    It's easier to use sqlalchemy core than ORM for parallel operations.
    """

    def query(self, sql):
        """
        for parallel implementation, need to create separate engine 
        for each process
        """
        engine = create_engine(self.settings.other.path_database)
        res = pd.read_sql(sql, con=engine)
        engine.dispose()
        return res

    def get_labels(self):
        """
        query the labels table

        Returns
        ----------
        pd.DataFrame
        """
        return self.query(
            f"""
            SELECT * FROM {self.settings.other.db_schema}.labels
            """
        )

    def get_inverted_index(self, names, table):
        """
        see dedupe.block.learner.InvertedIndex;

        Given forward index, construct inverted index. 
        Then for each row in inverted index, get all "nC2" distinct 
        combinations of size 2 from the array. 
        
        Returns inverted Index

        Parameters
        ----------
        names : List[str]
            list of block schemes
        table : str
            table name of forward index

        Returns
        ----------
        pd.DataFrame
        """
        return self.query(
            f"""
            WITH 
                inverted_index AS (
                    SELECT 
                        {signatures(names)}, 
                        ARRAY_AGG(_index ORDER BY _index asc) as array_agg
                    FROM {self.settings.other.db_schema}.{table}
                    GROUP BY {", ".join([f"signature{i}" for i in range(len(names))])}
                )
            SELECT * 
            FROM inverted_index
            WHERE array_length(array_agg, 1) > 1
            """
        )

    def get_inverted_index_pairs(self, names, table):
        """
        see dedupe.block.learner.InvertedIndex;

        Given forward index, construct inverted index. 
        Then for each row in inverted index, get all "nC2" distinct 
        combinations of size 2 from the array. 
        
        Concatenates and returns all distinct pairs.

        Parameters
        ----------
        names : List[str]
            list of block schemes
        table : str
            table name of forward index

        Returns
        ----------
        pd.DataFrame
        """
        return self.query(f"""
            WITH 
                inverted_index AS (
                    SELECT 
                        {signatures(names)}, 
                        ARRAY_AGG(_index ORDER BY _index asc) as array_agg
                    FROM {self.settings.other.db_schema}.{table}
                    GROUP BY {", ".join([f"signature{i}" for i in range(len(names))])}
                ),
                inverted_index_subset AS (
                    SELECT unnest_2d_1d(combinations(array_agg)) as pairs
                    FROM inverted_index
                    WHERE array_length(array_agg, 1) > 1
                )
            SELECT pairs[1] as _index_l, pairs[2] as _index_r, True as blocked
            FROM inverted_index_subset
            """)

    @cached_property
    def blocking_schemes(self):
        """
        Get all blocking schemes

        Returns
        ----------
        List[str]
        """
        return self.query(
            f"SELECT * FROM {self.settings.other.db_schema}.blocks_train LIMIT 1"
        ).columns[1:].tolist()

@dataclass
class Engine:
    settings: Settings
    """
    manages non-ORM textual connections to database
    """

    @cached_property
    def engine(self):
        return create_engine(self.settings.other.path_database)

@dataclass
class DatabaseORM(Tables, DatabaseCore, Engine):
    settings: Settings
    """
    Object to query database using sqlalchemy ORM. 
    Uses the Session object as interface to the database.
    """

    def get_train(self):
        """
        query the train table

        Returns
        ----------
        pd.DataFrame
        """
        with self.Session() as session:
            query = session.query(self.Train)
            return pd.read_sql(query.statement, query.session.bind)

    def get_distances(self):
        """
        query unlabelled distances for sample data

        Returns
        ----------
        pd.DataFrame
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
        query distances for full data

        Returns
        ----------
        pd.DataFrame
        """
        with self.Session() as session:
            query = (
                session
                .query(
                    *(
                        getattr(self.FullDistances,x) 
                        for x in self.settings.other.attributes
                    )
                )
                )
            return pd.read_sql(query.statement, query.session.bind)

    def get_full_comparison_indices(self):
        """
        query indices of comparison pairs for full data

        Returns
        ----------
        pd.DataFrame
        """
        with self.Session() as session:
            query = (
                session
                .query(self.FullDistances._index_l, self.FullDistances._index_r)
                .order_by(self.FullDistances._index_l, self.FullDistances._index_r)
                )
            return pd.read_sql(query.statement, query.session.bind)

    def get_compare_cols(self):
        """
        gets comparison columns with "_l" and "_r" suffices

        Returns
        ----------
        List[str]

        Examples
        ----------
        >>> self.settings.other.attributes = ["name", "address"]
        >>> get_compare_cols()
        ["name_l", "address_l", "index_l", "name_r", "address_r", "index_r"]
        """
        columns = [
            [f"{x}_l" for x in self.settings.other.attributes], 
            ["_index_l"],
            [f"{x}_r" for x in self.settings.other.attributes],
            ["_index_r"]
        ]
        return sum(columns, [])

    @property
    def fields_table(self):
        return {
            "comparisons":self.Sample,
            "full_comparisons":self.maindf,
            "labels": self.Train
        }
        
    def labcol(self, table):
        if "comparisons" in table.__tablename__:
            return table._index_l.label("drop")
        return table.label

    def get_comparison_attributes(self, table):
        """
        merge attributes on to dataframe with just comparison pair indices
        assign "_l" and "_r" suffices

        Returns
        ----------
        pd.DataFrame
        """

        data = self.fields_table[table.__tablename__]

        with self.Session() as session:
            dataL = aliased(data)
            dataR = aliased(data)
            query = (
                session
                .query(
                    self.labcol(table),
                    *(
                        getattr(dataL,x).label(f"{x}_l")
                        for x in self.settings.other.attributes + ["_index"]
                    ),
                    *(
                        getattr(dataR,x).label(f"{x}_r")
                        for x in self.settings.other.attributes + ["_index"]
                    ),
                )
                .outerjoin(dataL, table._index_l==dataL._index)
                .outerjoin(dataR, table._index_r==dataR._index)
                .order_by(table._index_l, table._index_r)
                )

            return (
                pd.read_sql(query.statement, query.session.bind)
                .drop(["drop"], axis=1, errors='ignore')
            )

    def get_clusters(self):
        with self.Session() as session:
            query = (
                session
                .query(self.Clusters.cluster, self.maindf)
                .join(self.maindf, self.Clusters._index == self.maindf._index)
                .order_by(self.Clusters.cluster)
                )
            return pd.read_sql(query.statement, query.session.bind)

    def truncate_table(self, table):
        self.engine.execute(f"""
            TRUNCATE TABLE {self.settings.other.db_schema}.{table};
        """)

    def _update_table(self, df, to_table):
        with self.Session() as session:    
            for r in df.to_dict(orient="records"):
                for k in r.keys():
                    setattr(to_table, k, r[k])
                session.merge(to_table)
            session.commit()
        
    def bulk_insert(self, df, to_table):
        with self.Session() as session:
            session.bulk_insert_mappings(
                to_table, 
                df.to_dict(orient='records')
            )
            session.commit()