""" This module computes distance calculations between comparison pairs.
"""

from dataclasses import dataclass

from dependency_injector.wiring import Provide
from sqlalchemy import create_engine, func, insert, select
from sqlalchemy.orm import aliased

from oagdedupe import utils as du
from oagdedupe._typing import SESSION, SUBQUERY, TABLE
from oagdedupe.containers import Container
from oagdedupe.db.orm import DatabaseORM
from oagdedupe.settings import Settings


@dataclass
class AllJaro(DatabaseORM):
    """
    Interface to compute distance between comparison pairs along
    common attributes.
    """

    settings: Settings = Provide[Container.settings]

    def __post_init__(self):
        self.orm = DatabaseORM(settings=self.settings)

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

    def save_comparison_attributes(self, table: TABLE, newtable: TABLE) -> None:
        """
        merge attributes on to dataframe with just comparison pair indices
        assign "_l" and "_r" suffices

        Returns
        ----------
        pd.DataFrame
        """

        with self.Session() as session:
            subquery = self.get_attributes(session, table)
            distance_query = self.compute_distances(subquery)

            stmt = insert(newtable).from_select(
                self.settings.attributes + self.compare_cols + ["label"],
                distance_query,
            )

            session.execute(str(stmt) + " ON CONFLICT DO NOTHING")
            session.commit()

    def save_distances(self, table: TABLE, newtable: TABLE) -> None:
        """
        get comparison attributes from table then compute distances
        for each pair

        Parameters
        ----------
        table: TABLE
        """

        # reset table
        self.orm.engine.execute(
            f"""
        TRUNCATE TABLE {self.settings.db.db_schema}.{newtable.__tablename__};
        """
        )

        self.save_comparison_attributes(table, newtable)
