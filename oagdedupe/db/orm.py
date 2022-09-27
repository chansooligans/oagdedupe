""" this module contains orm to interact with database; used for 
general queries and database modification
"""

from oagdedupe.settings import Settings
from oagdedupe.db.tables import Tables
from dataclasses import dataclass
import pandas as pd

@dataclass
class DatabaseORM(Tables):
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

    def get_labels(self):
        """
        query the labels table

        Returns
        ----------
        pd.DataFrame
        """
        with self.Session() as session:
            query = session.query(self.LabelsDistances)
            return pd.read_sql(query.statement, query.session.bind)

    def get_distances(self):
        """
        query unlabelled distances for sample data

        Returns
        ----------
        pd.DataFrame
        """
        with self.Session() as session:
            q = (session
                .query(self.Distances)
                .join(
                    self.LabelsDistances, 
                    (self.Distances._index_l==self.LabelsDistances._index_l) & 
                    (self.Distances._index_r==self.LabelsDistances._index_r), 
                    isouter=True)
                .filter(self.LabelsDistances.label == None))
            return pd.read_sql(q.statement, q.session.bind)

    def get_full_distances(self):
        """
        query distances for full data

        Returns
        ----------
        pd.DataFrame
        """
        with self.Session() as session:
            q = (session
                .query(*(getattr(self.FullDistances,x) 
                        for x in self.settings.other.attributes)))
            return pd.read_sql(q.statement, q.session.bind)

    def get_full_comparison_indices(self):
        """
        query indices of comparison pairs for full data

        Returns
        ----------
        pd.DataFrame
        """
        with self.Session() as session:
            q = (session
                .query(self.FullDistances._index_l, 
                        self.FullDistances._index_r)
                .order_by(self.FullDistances._index_l, 
                            self.FullDistances._index_r))
            return pd.read_sql(q.statement, q.session.bind)

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
            q = (session
                .query(self.maindf, self.Clusters.cluster)
                .outerjoin(
                    self.Clusters, self.Clusters._index == self.maindf._index)
                .order_by(self.Clusters.cluster))
            return pd.read_sql(q.statement, q.session.bind)

    def get_clusters_link(self):
        with self.Session() as session:
            dflist = []
            for _type in [True, False]:
                sq = (session
                    .query(self.Clusters.cluster,
                        self.Clusters._index,
                        self.Clusters._type)
                    .filter(self.Clusters._type==_type)
                    .subquery())
                q = (session
                    .query(self.maindf_link, sq.c.cluster)
                    .outerjoin(
                        sq, sq.c._index["_index"] == self.maindf_link._index)
                    .order_by(sq.c.cluster))
                dflist.append(pd.read_sql(q.statement, q.session.bind))
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