import itertools
import logging
from dataclasses import dataclass
from typing import List

from dependency_injector.wiring import Provide
from sqlalchemy import delete, func, select

from oagdedupe import utils as du
from oagdedupe._typing import SESSION, TABLE
from oagdedupe.db.base import BaseInitializeRepository
from oagdedupe.db.postgres import funcs
from oagdedupe.db.postgres.tables import Tables
from oagdedupe.settings import Settings


@dataclass
class InitializeRepository(BaseInitializeRepository, Tables):
    """
    Object used to initialize SQL tables using sqlalchemy

    Can be used to create:
        - df
        - pos/neg
            - created to help build train and labels;
            - pos contains a random sample repeated 4 times
            - neg contains 10 random samples
        - unlabelled
            - random sample of df of size settings.model.n,
            - samples are drawn each active learning loop
        - train
            - combines pos, neg, and unlabelled
        - labels
            - gets all distinct pairwise comparisons from train
            - pairs from pos are labelled as a match
            - pairs from neg are labelled as a non-match
    """

    settings: Settings

    @du.recordlinkage_repeat
    def _init_df(self, df=None, df_link=None, rl: str = "") -> None:
        """load df and/or df_link"""
        logging.info("building %s", f"df{rl}")
        if "_index" in locals()["df"].columns:
            raise ValueError("_index cannot be a column name")
        self.bulk_insert(
            df=locals()[f"df{rl}"],
            to_table=getattr(self, f"maindf{rl}"),
        )

    def _sample(self, session: SESSION, table: TABLE, n: int) -> List[dict]:
        """samples from df or df_link"""
        data = session.query(table).order_by(func.random()).limit(n).all()
        return self._to_dicts(data)

    def _to_dicts(self, data):
        return [
            {
                key: val
                for key, val in d.__dict__.items()
                if key != "_sa_instance_state"
            }
            for d in data
        ]

    def _init_pos(self, session: SESSION) -> None:
        """get positive samples: 4 copies of single sample"""
        records = self._sample(session, self.maindf, 1)
        for i in range(-3, 1):
            table = self.Pos(**records[0])
            if i < 0:
                setattr(table, "_index", i)
            setattr(table, "labelled", True)
            session.add(table)
        session.commit()

    @du.recordlinkage_repeat
    def _init_neg(self, session: SESSION, rl: str = "") -> None:
        """get negative samples: 10 random samples"""
        records = self._sample(session, getattr(self, f"maindf{rl}"), 10)
        for r in records:
            table = getattr(self, f"Neg{rl}")(**r)
            setattr(table, "labelled", True)
            session.add(table)
        session.commit()

    @du.recordlinkage_repeat
    def _init_unlabelled(self, session: SESSION, rl: str = "") -> None:
        """create unlabelled samples: 'n' random samples"""
        records = self._sample(
            session, getattr(self, f"maindf{rl}"), self.settings.model.n
        )
        for r in records:
            table = getattr(self, f"Unlabelled{rl}")(**r)
            setattr(table, "labelled", False)
            session.add(table)
        session.commit()

    @du.recordlinkage_repeat
    def _init_train(self, session: SESSION, rl: str = "") -> None:
        """create train by concatenating positive, negative,
        and unlabelled samples"""
        logging.info("building %s", f"train{rl}")
        fakedata = [
            getattr(self, f"Unlabelled{rl}"),
            self.Pos,
            getattr(self, f"Neg{rl}"),
        ]
        for tab in fakedata:
            records = self._to_dicts(session.query(tab).all())
            for r in records:
                table = getattr(self, f"Train{rl}")(**r)
                session.merge(table)
        session.commit()

    def _init_labels(self, session: SESSION) -> None:
        """create labels using positive and negative samples
        if positive, set "label" = 1
        if negative, set "label" = 0
        """
        logging.info("building %s", "labels")
        fakepairs = [(1, self.Pos), (0, self.Neg)]
        for lab, tab in fakepairs:
            records = session.query(tab).all()
            pairs = list(itertools.combinations(records, 2))
            for left, right in pairs:
                label = self.Labels()
                if left._index < right._index:
                    for attr in self.settings.attributes + ["_index"]:
                        du.inherit_attr(label, left, attr, attr + "_l")
                        du.inherit_attr(label, right, attr, attr + "_r")
                    label.label = lab
                    session.add(label)
        session.commit()

    def _init_labels_link(self, session: SESSION) -> None:
        """create labels for record linkage using positive and negative samples
        if positive, link to itself, set "label" = 1
        if negative, link neg to neg_link, set "label" = 0
        """
        logging.info("building %s", "labels")
        fakepairs = [(1, self.Pos, self.Pos), (0, self.Neg, self.Neg_link)]
        for lab, tab, tab_link in fakepairs:
            records = session.query(tab).all()
            records_link = session.query(tab_link).all()
            for left, right in zip(records, records_link):
                label = self.Labels()
                for attr in self.settings.attributes + ["_index"]:
                    du.inherit_attr(label, left, attr, attr + "_l")
                    du.inherit_attr(label, right, attr, attr + "_r")
                label.label = lab
                session.add(label)
        session.commit()

    @du.recordlinkage_repeat
    def _delete_unlabelled_from_train(
        self, session: SESSION, rl: str = ""
    ) -> None:
        """delete unlabelled from train"""
        stmt = delete(getattr(self, f"Train{rl}")).where(
            getattr(self, f"Train{rl}").labelled == False
        )
        session.execute(stmt)
        session.commit()

    @du.recordlinkage_repeat
    def _truncate_unlabelled(self, rl: str = ""):
        self.engine.execute(
            f"""
                TRUNCATE TABLE {self.settings.db.db_schema}.unlabelled{rl};
            """
        )

    @du.recordlinkage_repeat
    def resample_unlabelled(self, session: SESSION, rl: str = "") -> None:
        """delete unlabelled from train"""
        records = self._to_dicts(
            session.query(getattr(self, f"Unlabelled{rl}")).all()
        )
        for r in records:
            train = getattr(self, f"Train{rl}")(**r)
            session.merge(train)
        session.commit()

    @du.recordlinkage_repeat
    def _init_forward_index_full(self, rl: str = "") -> None:
        """initialize full index table

        (only required for sql implementation)
        """
        self.engine.execute(
            f"""
            DROP TABLE IF EXISTS {self.settings.db.db_schema}.blocks_df{rl};

            CREATE TABLE {self.settings.db.db_schema}.blocks_df{rl} as (
                SELECT
                    _index
                FROM {self.settings.db.db_schema}.df{rl}
            );
        """
        )

    def resample(self) -> None:
        """resample unlabelled from train"""
        with self.Session() as session:
            self._delete_unlabelled_from_train(session=session)
            self._truncate_unlabelled()
            self._init_unlabelled(session=session)
            self.resample_unlabelled(session=session)
            self._init_forward_index_full()
            # reset table
            for table in [
                "clusters",
                "comparisons",
            ]:
                self.engine.execute(
                    f"""
                    TRUNCATE TABLE {self.settings.db.db_schema}.{table};
                """
                )

    @du.recordlinkage
    def setup(self, df, df2=None, rl: str = "") -> None:
        """
        runs table creation functions

        Parameters
        ----------
        df: Optional[pd.DataFrame]
            dataframe to dedupe
        """

        funcs.create_functions(settings=self.settings)

        with self.Session() as session:
            logging.info(f"building schema: {self.settings.db.db_schema}")
            self.reset_tables()
            self._init_df(df=df, df_link=df2)
            self._init_pos(session)
            self._init_neg(session)
            self._init_unlabelled(session)
            self._init_train(session)
            getattr(self, f"_init_labels{rl}")(session)
            self._init_forward_index_full()
