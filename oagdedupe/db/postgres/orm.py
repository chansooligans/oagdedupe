""" this module contains orm to interact with database; used for
general queries and database modification
"""

import json
from dataclasses import dataclass
from typing import List

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine, func, insert, select, types, update
from sqlalchemy.orm import aliased
from tqdm import tqdm

from oagdedupe import utils as du
from oagdedupe._typing import SESSION, SUBQUERY, TABLE
from oagdedupe.db.base import (BaseClusterRepository, BaseDistanceRepository,
                               BaseFapiRepository)
from oagdedupe.db.postgres.tables import Tables
from oagdedupe.settings import Settings


@dataclass
class DistanceRepository(BaseDistanceRepository, Tables):
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

    def save_distances(self, full: bool, labels: bool) -> None:
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
                self.settings.attributes
                + self.settings.compare_cols
                + ["label"],
                distance_query,
            )

            session.execute(str(stmt) + " ON CONFLICT DO NOTHING")
            session.commit()


@dataclass
class ClusterRepository(BaseClusterRepository, Tables):
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


@dataclass
class FapiRepository(BaseFapiRepository, Tables):
    def predict(self, dists):
        return json.loads(
            requests.post(
                f"{self.settings.fast_api.url}/predict",
                json={"dists": dists.tolist()},
            ).content
        )

    def full_distance_partitions(self) -> select:
        return select(
            *(
                getattr(self.FullDistances, x)
                for x in self.settings.attributes + ["_index_l", "_index_r"]
            )
        ).execution_options(yield_per=50000)

    def update_train(self, newlabels: pd.DataFrame) -> None:
        """
        for entities that were labelled,
        set "labelled" column in train table to True
        """
        indices = set(newlabels["_index_l"]).union(set(newlabels["_index_r"]))
        with self.Session() as session:
            stmt = (
                update(self.Train)
                .where(self.Train._index.in_(indices))
                .values(labelled=True)
            )
            session.execute(stmt)
            session.commit()

    def update_labels(self, newlabels: pd.DataFrame) -> None:
        """
        add new labels to labels table
        """
        self._update_table(newlabels, self.Labels())

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

    def save_predictions(self):
        with self.Session() as session:

            stmt = self.full_distance_partitions()

            for i, partition in tqdm(
                enumerate(session.execute(stmt).partitions())
            ):

                dists = np.array(
                    [
                        [
                            getattr(row, x)
                            for x in self.settings.attributes
                            + ["_index_l", "_index_r"]
                        ]
                        for row in partition
                    ]
                )

                preds = np.array(self.predict(dists))

                probs = pd.DataFrame(
                    np.hstack([preds[:, 1:], dists[:, -2:]]),
                    columns=["score", "_index_l", "_index_r"],
                )

                probs.to_sql(
                    "scores",
                    schema=self.settings.db.db_schema,
                    if_exists="append" if i > 0 else "replace",
                    con=self.engine,
                    index=False,
                    dtype={
                        "_index_l": types.Integer(),
                        "_index_r": types.Integer(),
                    },
                )
