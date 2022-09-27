from oagdedupe.settings import Settings
from oagdedupe.distance.string import AllJaro
from oagdedupe.db.orm import DatabaseORM
from sqlalchemy import select, delete, func
from oagdedupe import utils as du

import itertools
from dataclasses import dataclass
import logging

@dataclass
class Initialize(DatabaseORM):
    """
    Object used to initialize SQL tables using sqlalchemy

    Can be used to create:
        - df
        - pos/neg
            - created to help build train and labels; 
            - pos contains a random sample repeated 4 times
            - neg contains 10 random samples
        - unlabelled
            - random sample of df of size settings.other.n, 
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
    def _init_df(self, df=None, df_link=None, rl=""):
        """load df and/or df_link"""
        if "_index" in locals()["df"].columns:
            raise ValueError("_index cannot be a column name")
        self.bulk_insert(
            df=locals()[f"df{rl}"],
            to_table=getattr(self, f"maindf{rl}"),
        )

    def _sample(self, session, table, n):
        """samples from df or df_link"""
        return (
            session
            .query(table)
            .order_by(func.random())
            .limit(n)
            .all()
        )

    def _init_pos(self, session):
        """get positive samples: 4 copies of single sample"""
        records = self._sample(session, self.maindf, 1)
        for i in range(-3,1):
            table = self.Pos()
            for attr in self.settings.other.attributes + ["_index"]:
                setattr(table, attr, getattr(records[0], attr))
            if i < 0:
                setattr(table, "_index", i)
            setattr(table, "labelled", True)
            session.add(table)
        session.commit()

    @du.recordlinkage_repeat
    def _init_neg(self, session, rl=""):
        """get negative samples: 10 random samples"""
        records = self._sample(session, getattr(self, f"maindf{rl}"), 10)
        for r in records:
            table = getattr(self,f"Neg{rl}")()
            for attr in self.settings.other.attributes + ["_index"]:
                setattr(table, attr, getattr(r, attr))
            setattr(table, "labelled", True)
            session.add(table)
        session.commit()

    @du.recordlinkage_repeat
    def _init_unlabelled(self, session, rl=""):
        """ create unlabelled samples: 'n' random samples"""
        records = self._sample(
            session, getattr(self, f"maindf{rl}"), self.settings.other.n)
        for r in records:
            table = getattr(self,f"Unlabelled{rl}")()
            for attr in self.settings.other.attributes + ["_index"]:
                setattr(table, attr, getattr(r, attr))
            setattr(table, "labelled", False)
            session.add(table)
        session.commit()

    @du.recordlinkage_repeat
    def _init_train(self, session, rl=""): 
        """create train by concatenating positive, negative, 
        and unlabelled samples"""
        fakedata = [
            getattr(self, f"Unlabelled{rl}"), 
            self.Pos, 
            getattr(self,f"Neg{rl}")
        ]
        for tab in fakedata:
            records = session.query(tab).all()
            for r in records:
                table = getattr(self,f"Train{rl}")()
                for attr in self.settings.other.attributes + ["_index", "labelled"]:
                    setattr(table, attr, getattr(r, attr))
                session.merge(table)
        session.commit()

    def _init_labels(self, session):
        """create labels using positive and negative samples
        if positive, set "label" = 1
        if negative, set "label" = 0
        """
        fakepairs = [(1, self.Pos), (0, self.Neg)]
        for l,tab in fakepairs:
            records = session.query(tab).all()
            pairs = list(itertools.combinations(records, 2))
            for left,right in pairs:
                label = self.Labels()
                if left._index < right._index:
                    label._index_l = left._index
                    label._index_r = right._index
                    label.label = l
                    session.add(label)
        session.commit()
        self._label_distances()

    def _init_labels_link(self, session):
        """create labels for record linkage using positive and negative samples
        if positive, link to itself, set "label" = 1
        if negative, link neg to neg_link, set "label" = 0
        """
        fakepairs = [(1, self.Pos, self.Pos), (0, self.Neg, self.Neg_link)]
        for l,tab,tab_link in fakepairs:
            records = session.query(tab).all()
            records_link = session.query(tab_link).all()
            for left,right in zip(records, records_link):
                label = self.Labels()
                label._index_l = left._index
                label._index_r = right._index
                label.label = l
                session.add(label)
        session.commit()
        self._label_distances()

    def _label_distances(self):
        """
        computes distances between pairs of records from labels table;
        """
        self.distance = AllJaro(settings=self.settings)
        self.distance.save_distances(
            table=self.Labels,
            newtable=self.LabelsDistances
        )

    @du.recordlinkage_repeat
    def _delete_unlabelled(self, session, rl=""):
        """delete unlabelled from train"""
        stmt = (
            delete(getattr(self,f"Train{rl}")).
            where(getattr(self,f"Train{rl}").labelled==False)
        )
        session.execute(stmt)
        session.commit()

    @du.recordlinkage_repeat
    def _resample_unlabelled(self, session, rl=""):
        """delete unlabelled from train"""
        self.engine.execute(f"""
                TRUNCATE TABLE {self.settings.other.db_schema}.unlabelled{rl};
            """)
        records = session.query(getattr(self,f"Unlabelled{rl}")).all()
        for r in records:
            train = getattr(self,f"Train{rl}")()
            for attr in self.settings.other.attributes + ["_index", "labelled"]:
                setattr(train, attr, getattr(r, attr))
            session.merge(train)
        session.commit()

    @du.recordlinkage_repeat
    def _resample(self, session, rl=""):
        """resample unlabelled from train"""
        self._delete_unlabelled(session)
        self._resample_unlabelled(session)
    
    @du.recordlinkage
    def setup(self, df=None, df2=None, reset=True, resample=False, rl=""):
        """
        runs table creation functions

        Parameters
        ----------
        df: Optional[pd.DataFrame]
            dataframe to dedupe
        reset: bool
            set True to delete and create all tables
        resample: bool
            used for active learning loops where model needs to pull a new 
            sample, without deleting df, train, or labels
        """
        
        # initialize Tables sqlalchemy classes
        self.setup_dynamic_declarative_mapping()

        with self.Session() as session:
            if reset:
                logging.info("building database")
                self.reset_tables()
                self._init_df(df=df, df_link=df2)
                self._init_pos(session)
                self._init_neg(session)
                self._init_unlabelled(session)
                self._init_train(session)
                getattr(self, f"_init_labels{rl}")(session)

            if resample:
                logging.info("resampling train")
                self._resample(session)
                self._label_distances()
