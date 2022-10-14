""" this module contains orm to interact with database; used for
general queries and database modification
"""

from dataclasses import dataclass
from typing import List

import numpy as np
import pandas as pd
from dependency_injector.wiring import Provide
from sqlalchemy import create_engine, func, insert, select
from sqlalchemy.orm import aliased
from tqdm import tqdm

from oagdedupe import utils as du
from oagdedupe._typing import SESSION, SUBQUERY, TABLE
from oagdedupe.containers import Container
from oagdedupe.db.postgres.tables import Tables
from oagdedupe.settings import Settings


@dataclass
class DatabaseORM(Tables):
    """
    Object to query database using sqlalchemy ORM.
    Uses the Session object as interface to the database.
    """

    settings: Settings = Provide[Container.settings]

    ########################################################################
    # queries
    ########################################################################

    def get_train(self) -> pd.DataFrame:
        """
        query the train table

        Returns
        ----------
        pd.DataFrame
        """
        with self.Session() as session:
            query = session.query(self.Train)
            return pd.read_sql(query.statement, query.session.bind)

    def get_labels(self) -> pd.DataFrame:
        """
        query the labels table

        Returns
        ----------
        pd.DataFrame
        """
        with self.Session() as session:
            query = session.query(self.LabelsDistances)
            return pd.read_sql(query.statement, query.session.bind)

    def get_distances(self) -> pd.DataFrame:
        """
        query unlabelled distances for sample data

        Returns
        ----------
        pd.DataFrame
        """
        with self.Session() as session:
            q = (
                session.query(self.Distances)
                .join(
                    self.LabelsDistances,
                    (self.Distances._index_l == self.LabelsDistances._index_l)
                    & (
                        self.Distances._index_r == self.LabelsDistances._index_r
                    ),
                    isouter=True,
                )
                .filter(self.LabelsDistances.label == None)
            )
            return pd.read_sql(q.statement, q.session.bind)

    def get_full_distances(self) -> pd.DataFrame:
        """
        query distances for full data

        Returns
        ----------
        pd.DataFrame
        """
        with self.Session() as session:
            q = session.query(
                *(
                    getattr(self.FullDistances, x)
                    for x in self.settings.attributes
                )
            )
            return pd.read_sql(q.statement, q.session.bind)

    def full_distance_partitions(self) -> select:
        return select(
            *(
                getattr(self.FullDistances, x)
                for x in self.settings.attributes + ["_index_l", "_index_r"]
            )
        ).execution_options(yield_per=50000)

    def get_full_comparison_indices(self) -> pd.DataFrame:
        """
        query indices of comparison pairs for full data

        Returns
        ----------
        pd.DataFrame
        """
        with self.Session() as session:
            q = session.query(
                self.FullDistances._index_l, self.FullDistances._index_r
            ).order_by(self.FullDistances._index_l, self.FullDistances._index_r)
            return pd.read_sql(q.statement, q.session.bind)

    @property
    def compare_cols(self) -> List[str]:
        """
        gets comparison columns with "_l" and "_r" suffices

        Returns
        ----------
        List[str]

        Examples
        ----------
        >>> self.settings.attributes = ["name", "address"]
        >>> compare_cols()
        [
            "name_l", "address_l", "name_r", "address_r",
            "_index_l", "_index_r"
        ]
        """
        columns = [
            [f"{x}_l" for x in self.settings.attributes],
            [f"{x}_r" for x in self.settings.attributes],
            ["_index_l", "_index_r"],
        ]
        return sum(columns, [])

    ########################################################################
    # String Distance Computations
    ########################################################################
    @du.recordlinkage
    def fields_table(self, table: str, rl: str = "") -> tuple:
        mapping = {
            "comparisons": "Train",
            "full_comparisons": "maindf",
            "labels": "Train",
        }

        return (
            aliased(getattr(self, mapping[table])),
            aliased(getattr(self, mapping[table] + rl)),
        )

    def get_attributes(self, session: SESSION, table: TABLE) -> SUBQUERY:
        dataL, dataR = self.fields_table(table.__tablename__)
        return (
            session.query(
                *(
                    getattr(dataL, x).label(f"{x}_l")
                    for x in self.settings.attributes
                ),
                *(
                    getattr(dataR, x).label(f"{x}_r")
                    for x in self.settings.attributes
                ),
                table._index_l,
                table._index_r,
                table.label,
            )
            .outerjoin(dataL, table._index_l == dataL._index)
            .outerjoin(dataR, table._index_r == dataR._index)
            .order_by(table._index_l, table._index_r)
        ).subquery()

    def compute_distances(self, subquery: SUBQUERY) -> select:
        return select(
            *(
                func.jarowinkler(
                    getattr(subquery.c, f"{attr}_l"),
                    getattr(subquery.c, f"{attr}_r"),
                ).label(attr)
                for attr in self.settings.attributes
            ),
            subquery,
        )

    def save_comparison_attributes_dists(
        self, full: bool, labels: bool
    ) -> None:
        """
        merge attributes on to dataframe with just comparison pair indices
        assign "_l" and "_r" suffices

        Returns
        ----------
        pd.DataFrame
        """
        if labels:
            table = self.Labels
            newtable = self.LabelsDistances
        else:
            if full:
                table = self.FullComparisons
                newtable = self.FullDistances
            else:
                table = self.Comparisons
                newtable = self.Distances

        with self.Session() as session:
            subquery = self.get_attributes(session, table)
            distance_query = self.compute_distances(subquery)

            stmt = insert(newtable).from_select(
                self.settings.attributes + self.compare_cols + ["label"],
                distance_query,
            )

            session.execute(str(stmt) + " ON CONFLICT DO NOTHING")
            session.commit()

    ########################################################################
    # Clustering Computations
    ########################################################################

    def get_scores(self, threshold) -> pd.DataFrame:
        return pd.read_sql(
            f"""
            SELECT * FROM {self.settings.db.db_schema}.scores
            WHERE score > {threshold}""",
            con=self.engine,
        )

    def get_clusters(self) -> pd.DataFrame:
        """
        adds cluster IDs to df

        Returns
        ----------
        pd.DataFrame
        """
        with self.Session() as session:
            q = (
                session.query(self.maindf, self.Clusters.cluster)
                .outerjoin(
                    self.Clusters, self.Clusters._index == self.maindf._index
                )
                .order_by(self.Clusters.cluster)
            )
            return pd.read_sql(q.statement, q.session.bind)

    def _cluster_subquery(self, session: SESSION, _type: bool) -> SUBQUERY:
        """
        subquery in get_clsuters_link(); filters clusters to either df or df_link
        entities

        Returns
        ----------
        SUBQUERY
        """
        return (
            session.query(
                self.Clusters.cluster,
                self.Clusters._index,
                self.Clusters._type,
            )
            .filter(self.Clusters._type == _type)
            .subquery()
        )

    def get_clusters_link(self) -> List[pd.DataFrame]:
        """
        adds cluster IDs to df and df_link

        Returns
        ----------
        List[pd.DataFrame]
        """
        maindf = {True: self.maindf, False: self.maindf_link}
        with self.Session() as session:
            dflist = []
            for _type in [True, False]:
                sq = self._cluster_subquery(session=session, _type=_type)
                q = (
                    session.query(maindf[_type], sq.c.cluster)
                    .outerjoin(
                        sq, sq.c._index["_index"] == maindf[_type]._index
                    )
                    .order_by(sq.c.cluster)
                )
                dflist.append(pd.read_sql(q.statement, q.session.bind))
            return dflist

    def merge_clusters_with_raw_data(self, df_clusters, rl):

        self.bulk_insert(df=df_clusters, to_table=self.Clusters)

        if rl == "":
            return self.get_clusters()
        else:
            return self.get_clusters_link()
