from dedupe.settings import Settings
from dedupe.distance.string import RayAllJaro
from dedupe.db.tables import Tables
from sqlalchemy import select, insert, func


from dataclasses import dataclass
import logging

@dataclass
class Initialize(Tables):
    settings:Settings
    reset:bool = True

    def _init_df(self, df):
        logging.info(f"Building table {self.settings.other.db_schema}.df.")
        with self.Session() as session:
            session.bulk_insert_mappings(
                self.maindf, 
                df.to_dict(orient='records')
            )
            session.commit()

    def _init_sample(self):
        logging.info(f"Building table {self.settings.other.db_schema}.sample.")
        with self.Session() as session:
            sample = (
                select([self.maindf])
                .order_by(func.random())
                .limit(self.settings.other.n)
            )
            session.execute(
                insert(self.Sample).from_select(sample.subquery(1).c, sample)
            )
            session.commit()

    def _init_pos(self):
        # create pos
        pos = select([self.maindf]).order_by(func.random()).limit(1)
        with self.Session() as session:
            res = session.execute(pos).first()
            for _ in range(4):
                pos = self.Pos()
                for attr in self.settings.other.attributes + ["_index"]:
                    setattr(pos, attr, getattr(res[0], attr))
                setattr(pos, "label", 1)
                session.add(pos)
            session.commit()

    def _init_neg(self):
        # create neg
        neg = select([self.maindf]).order_by(func.random()).limit(10)
        with self.Session() as session:
            records = session.execute(neg).all()
            for r in records:
                neg = self.Neg()
                for attr in self.settings.other.attributes + ["_index"]:
                    setattr(neg, attr, getattr(r[0], attr))
                setattr(neg, "label", 0)
                session.add(neg)
            session.commit()

    def _init_train(self):
        
        logging.info(f"Building table {self.settings.other.db_schema}.train.")
        self._init_pos()
        self._init_neg()
        
        # create train
        with self.Session() as session:
            for tab in [self.Pos, self.Neg]:
                records = session.query(tab).all()
                for r in records:
                    train = self.Train()
                    for attr in self.settings.other.attributes + ["_index"]:
                        setattr(train, attr, getattr(r, attr))
                    session.add(train)
            session.commit()


    def _init_labels(self):
        logging.info(f"Building table {self.settings.other.db_schema}.labels.")
        with self.Session() as session:
            for l,tab in [(1,self.Pos), (0,self.Neg)]:
                records = session.query(tab).all()
                for r in records:
                    label = self.Labels()
                    label._index_l = r._index
                    label._index_r = r._index
                    label.label = l
                    session.add(label)
            session.commit()

        self.distance = RayAllJaro(settings=self.settings)
        self.distance.save_distances(
            table="labels",
            newtable="labels"
        )

    def setup(self, df, reset):
        
        # initialize Tables sqlalchemy classes
        self.setup_dynamic_declarative_mapping()

        if reset:
            self.reset_tables()

        logging.info(f"building tables in schema: {self.settings.other.db_schema}")
        if df is not None:
            if "_index" in df.columns:
                raise ValueError("_index cannot be a column name")
            self._init_df(df=df)

        self._init_sample()
        self._init_train()
        self._init_labels()