""" This module computes distance calculations between comparison pairs.
"""

from dataclasses import dataclass
from jellyfish import jaro_winkler_similarity
import ray

from sqlalchemy.orm import aliased
from sqlalchemy import create_engine, select, func, insert

from oagdedupe.base import BaseDistance
from oagdedupe.db.orm import DatabaseORM
from oagdedupe.settings import Settings
from oagdedupe import utils as du


@dataclass
class AllJaro(DatabaseORM):
    """
    Interface to compute distance between comparison pairs along 
    common attributes.
    """
    settings: Settings

    def __post_init__(self):
        self.orm = DatabaseORM(settings=self.settings)

    @du.recordlinkage
    def fields_table(self, table, rl=""):
        mapping = {
            "comparisons":"Train",
            "full_comparisons":"maindf",
            "labels":"Train"
        }
        
        return (
            aliased(getattr(self,mapping[table])), 
            aliased(getattr(self,mapping[table]+rl))
        )
        

    def get_attributes(self, session, table):
        dataL, dataR = self.fields_table(table.__tablename__)
        return (
            session
            .query(
                *(
                    getattr(dataL,x).label(f"{x}_l")
                    for x in self.settings.other.attributes
                ),
                *(
                    getattr(dataR,x).label(f"{x}_r")
                    for x in self.settings.other.attributes
                ),
                table._index_l,
                table._index_r,
                table.label
            )
            .outerjoin(dataL, table._index_l==dataL._index)
            .outerjoin(dataR, table._index_r==dataR._index)
            .order_by(table._index_l, table._index_r)
        ).subquery()

    def get_distances(self, subquery):
        return select(
            *(
                func.jarowinkler(
                    getattr(subquery.c,f"{attr}_l"),
                    getattr(subquery.c,f"{attr}_r")
                ).label(attr)
                for attr in self.settings.other.attributes
            ),
            subquery
        )

    def save_comparison_attributes(self, table, newtable):
        """
        merge attributes on to dataframe with just comparison pair indices
        assign "_l" and "_r" suffices

        Returns
        ----------
        pd.DataFrame
        """

        with self.Session() as session:
            subquery = self.get_attributes(session, table)
            distance_query = self.get_distances(subquery)

            stmt = (
                insert(newtable)
                .from_select(
                    self.settings.other.attributes + self.compare_cols + ["label"], 
                    distance_query
                )
            )

            session.execute(str(stmt) + ' ON CONFLICT DO NOTHING')
            session.commit()

    def save_distances(self, table, newtable):
        """
        get comparison attributes from table then compute distances
        for each pair
        
        Parameters
        ----------
        table: sqlalchemy.orm.decl_api.DeclarativeMeta
        """

        # reset table
        self.orm.engine.execute(f"""
        TRUNCATE TABLE {self.settings.other.db_schema}.{newtable.__tablename__};
        """)

        self.save_comparison_attributes(table, newtable)
      