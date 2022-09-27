from oagdedupe.settings import Settings
from oagdedupe.db.tables import Tables
from oagdedupe import utils as du

from functools import cached_property
from sqlalchemy import create_engine, select, func, insert
from dataclasses import dataclass
import pandas as pd
import numpy as np

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
    """
    Object contains methods to query database using sqlalchemy CORE;
    It's easier to use sqlalchemy core than ORM for parallel operations.
    """
    settings: Settings

    def query(self, sql):
        """
        for parallel implementation, need to create separate engine 
        for each process
        """
        engine = create_engine(self.settings.other.path_database)
        res = pd.read_sql(sql, con=engine)
        engine.dispose()
        return res

    def truncate_table(self, table):
        engine = create_engine(self.settings.other.path_database)
        engine.execute(f"""
            TRUNCATE TABLE {self.settings.other.db_schema}.{table};
        """)
        engine.dispose()

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

    @property
    def comptab_map(self):
        return {
            "blocks_train":"comparisons",
            "blocks_df":"full_comparisons"
        }

    @du.recordlinkage
    def save_comparison_pairs(self, names, table, rl=""):
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
        aliases = [f"signature{i}" for i in range(len(names))]

        if rl == "":
            where = "WHERE t1._index_l < t2._index_r"
        else:
            where = ""

        newtable = self.comptab_map[table] 

        engine = create_engine(self.settings.other.path_database)
        engine.execute(f"""
            INSERT INTO {self.settings.other.db_schema}.{newtable}
            (
                WITH 
                    inverted_index AS (
                        SELECT 
                            {signatures(names)}, 
                            unnest(ARRAY_AGG(_index ORDER BY _index asc)) _index_l
                        FROM {self.settings.other.db_schema}.{table}
                        GROUP BY {", ".join(aliases)}
                    ),
                    inverted_index_link AS (
                        SELECT 
                            {signatures(names)}, 
                            unnest(ARRAY_AGG(_index ORDER BY _index asc)) _index_r
                        FROM {self.settings.other.db_schema}.{table}{rl}
                        GROUP BY {", ".join(aliases)}
                    )
                SELECT distinct _index_l, _index_r
                FROM inverted_index t1
                JOIN inverted_index_link t2
                    ON {" and ".join(
                        [f"t1.{s} = t2.{s}" for s in aliases]
                    )}
                {where}
                GROUP BY _index_l, _index_r
                HAVING count(*) = 1
            )
            ON CONFLICT DO NOTHING
            """
        )
        engine.dispose()

    def get_n_pairs(self, table):
        newtable = self.comptab_map[table]
        return self.query(f"""
            SELECT count(*) FROM {self.settings.other.db_schema}.{newtable}
        """)["count"].values[0]


    @du.recordlinkage
    def get_inverted_index_stats(self, names, table, rl=""):
        """
        Given forward index, construct inverted index. 
        Then for each row in inverted index, get all "nC2" distinct 
        combinations of size 2 from the array. Then compute
        number of pairs, the positive coverage and negative coverage.

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
        aliases = [f"signature{i}" for i in range(len(names))]

        if rl == "":
            where = "WHERE t1._index_l < t2._index_r"
        else:
            where = ""

        res = self.query(f"""
            WITH 
                inverted_index AS (
                    SELECT 
                        {signatures(names)}, 
                        unnest(ARRAY_AGG(_index ORDER BY _index asc)) _index_l
                    FROM {self.settings.other.db_schema}.{table}
                    GROUP BY {", ".join(aliases)}
                ),
                inverted_index_link AS (
                    SELECT 
                        {signatures(names)}, 
                        unnest(ARRAY_AGG(_index ORDER BY _index asc)) _index_r
                    FROM {self.settings.other.db_schema}.{table}{rl}
                    GROUP BY {", ".join(aliases)}
                ),
                labels AS (
                    SELECT _index_l, _index_r, label
                    FROM {self.settings.other.db_schema}.labels
                )
            SELECT 
                count(*) as n_pairs,
                SUM(
                    CASE WHEN t3.label = 1 THEN 1 ELSE 0 END
                ) positives,
                SUM(
                    CASE WHEN t3.label = 0 THEN 1 ELSE 0 END
                ) negatives
            FROM inverted_index t1
            JOIN inverted_index_link t2
                ON {" and ".join(
                    [f"t1.{s} = t2.{s}" for s in aliases]
                )}
            LEFT JOIN labels t3
                ON t3._index_l = t1._index_l
                AND t3._index_r = t2._index_r
            {where}
            """)

        return res.loc[0].to_dict()


    @cached_property
    def blocking_schemes(self):
        """
        Get all blocking schemes

        Returns
        ----------
        List[str]
        """
        return [
            tuple([x]) 
            for x in self.query(
                f"""
                SELECT * 
                FROM {self.settings.other.db_schema}.blocks_train LIMIT 1
                """
            ).columns[1:].tolist()
        ]

    @du.recordlinkage_both
    def n(self, rl=""):
        return self.query(
            f"SELECT count(*) FROM {self.settings.other.db_schema}.df{rl}"
        )["count"].values[0]

    @cached_property
    def n_comparisons(self):
        """number of total possible comparisons"""
        n = self.n()
        if self.settings.other.dedupe == False:
            return np.product(n)
        return (n * (n-1))/2
    
    @property
    def min_rr(self):
        """minimum reduction ratio"""
        reduced = (self.n_comparisons - self.settings.other.max_compare) 
        return reduced / self.n_comparisons


@dataclass
class Engine:
    """
    manages non-ORM textual connections to database
    """
    settings: Settings

    @cached_property
    def engine(self):
        return create_engine(self.settings.other.path_database)


@dataclass
class DatabaseORM(Tables, DatabaseCore):
    """
    Object to query database using sqlalchemy ORM. 
    Uses the Session object as interface to the database.
    """
    settings: Settings

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

    @property
    def compare_cols(self):
        """
        gets comparison columns with "_l" and "_r" suffices

        Returns
        ----------
        List[str]

        Examples
        ----------
        >>> self.settings.other.attributes = ["name", "address"]
        >>> compare_cols()
        [
            "name_l", "address_l", "name_r", "address_r", 
            "index_l", "index_r"
        ]
        """
        columns = [
            [f"{x}_l" for x in self.settings.other.attributes], 
            [f"{x}_r" for x in self.settings.other.attributes],
            ["_index_l","_index_r"]
        ]
        return sum(columns, [])

    def labcol(self, table):
        if "comparisons" in table.__tablename__:
            return table._index_l.label("drop")
        return table.label


    def get_clusters(self):
        with self.Session() as session:
            query = (
                session
                .query(self.maindf, self.Clusters.cluster)
                .outerjoin(self.Clusters, self.Clusters._index == self.maindf._index)
                .order_by(self.Clusters.cluster)
                )
            return pd.read_sql(query.statement, query.session.bind)

    def get_clusters_link(self):
        with self.Session() as session:
            dflist = []
            for _type in [True, False]:
                subquery = session.query(
                    self.Clusters.cluster,self.Clusters._index,self.Clusters._type
                ).filter(self.Clusters._type==_type).subquery()
                query = (
                    session
                    .query(self.maindf_link, subquery.c.cluster)
                    .outerjoin(subquery, subquery.c._index["_index"] == self.maindf_link._index)
                    .order_by(subquery.c.cluster)
                    )
                dflist.append(pd.read_sql(query.statement, query.session.bind))
            return dflist

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