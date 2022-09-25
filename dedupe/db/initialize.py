from oagdedupe.settings import Settings
from oagdedupe.distance.string import RayAllJaro
from oagdedupe.db.tables import Tables
from sqlalchemy import select, delete, func
from oagdedupe import utils as du

import itertools
from dataclasses import dataclass
import logging

@dataclass
class Initialize(Tables):
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
    settings:Settings

    def _init_df(self, df, rl=""):
        logging.info(f"Building table {self.settings.other.db_schema}.df{rl}")
        with self.Session() as session:
            session.bulk_insert_mappings(
                getattr(self,f"maindf{rl}"),
                df.to_dict(orient='records')
            )
            session.commit()

    def _init_pos(self):
        # create pos
        pos = select([self.maindf]).order_by(func.random()).limit(1)
        with self.Session() as session:
            records = session.execute(pos).first()
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
    def _init_neg(self, rl=""):
        # create neg
        neg = (
            select([getattr(self,f"maindf{rl}")]).order_by(func.random()).limit(10)
        )
        with self.Session() as session:
            records = session.execute(neg).all()
            for r in records:
                table = getattr(self,f"Neg{rl}")()
                for attr in self.settings.other.attributes + ["_index"]:
                    setattr(table, attr, getattr(r[0], attr))
                setattr(table, "labelled", True)
                session.add(table)
            session.commit()

    @du.recordlinkage_repeat
    def _init_unlabelled(self, rl=""):
        # create unlabelled
        unlabelled = (
            select([getattr(self, f"maindf{rl}")])
            .order_by(func.random())
            .limit(self.settings.other.n)
        )
        with self.Session() as session:
            records = session.execute(unlabelled).all()
            for r in records:
                table = getattr(self,f"Unlabelled{rl}")()
                for attr in self.settings.other.attributes + ["_index"]:
                    setattr(table, attr, getattr(r[0], attr))
                setattr(table, "labelled", False)
                session.add(table)
            session.commit()

    @du.recordlinkage_repeat
    def _init_train(self, rl=""):
        
        logging.info(f"Building table {self.settings.other.db_schema}.train{rl}")        
        # create train
        with self.Session() as session:
            for tab in [getattr(self,f"Unlabelled{rl}"), self.Pos, getattr(self,f"Neg{rl}")]:
                records = session.query(tab).all()
                for r in records:
                    train = getattr(self,f"Train{rl}")()
                    for attr in self.settings.other.attributes + ["_index", "labelled"]:
                        setattr(train, attr, getattr(r, attr))
                    session.merge(train)
            session.commit()

    def _init_labels(self):
        logging.info(f"Building table {self.settings.other.db_schema}.labels.")
        with self.Session() as session:
            for l,tab in [(1,self.Pos), (0,self.Neg)]:
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

    def _init_labels_link(self):
        logging.info(f"Building table {self.settings.other.db_schema}.labels_link.")
        with self.Session() as session:
            for l,tab,tab_link in [(1,self.Pos,self.Pos), (0,self.Neg,self.Neg_link)]:
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
        computes distances between pairs of records from labels table 
        then appends distances to the labels table
        """
        self.distance = RayAllJaro(settings=self.settings)
        self.distance.save_distances(
            table=self.Labels,
            newtable=self.Labels
        )

    @du.recordlinkage_repeat
    def _resample(self, rl=""):

        # delete unlabelled from train
        with self.Session() as session:
            stmt = (
                delete(getattr(self,f"Train{rl}")).
                where(getattr(self,f"Train{rl}").labelled==False)
            )
            session.execute(stmt)
            session.commit()

        # resample unlabelled
        self.engine.execute(f"""
                TRUNCATE TABLE {self.settings.other.db_schema}.unlabelled{rl};
            """)

        # add to train
        with self.Session() as session:
     
            records = session.query(getattr(self,f"Unlabelled{rl}")).all()
            for r in records:
                train = getattr(self,f"Train{rl}")()
                for attr in self.settings.other.attributes + ["_index", "labelled"]:
                    setattr(train, attr, getattr(r, attr))
                session.merge(train)
            session.commit()

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

        if reset:
            self.reset_tables()

            logging.info(f"building tables in schema: {self.settings.other.db_schema}")
            if "_index" in df.columns:
                raise ValueError("_index cannot be a column name")
            
            self._init_df(df=df)
            if self.settings.other.dedupe == False:
                self._init_df(df=df2, rl="_link")

            self._init_pos()
            self._init_neg()
            self._init_unlabelled()
            self._init_train()

            getattr(self, f"_init_labels{rl}")()

        if resample:
            self._resample()
            self._label_distances()

    