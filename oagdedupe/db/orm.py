""" this module contains orm to interact with database; used for
general queries and database modification
"""

from dataclasses import dataclass
from typing import List

import numpy as np
import pandas as pd
from dependency_injector.wiring import Provide
from sqlalchemy import select
from tqdm import tqdm

from oagdedupe._typing import SESSION, SUBQUERY
from oagdedupe.containers import Container
from oagdedupe.db.tables import Tables
from oagdedupe.settings import Settings


@dataclass
class DatabaseORM(Tables):
    """
    Object to query database using sqlalchemy ORM.
    Uses the Session object as interface to the database.
    """

    settings: Settings = Provide[Container.settings]

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
