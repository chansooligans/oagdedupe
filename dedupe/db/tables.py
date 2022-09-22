from dedupe.settings import Settings
from dataclasses import dataclass
from functools import cached_property
from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, create_engine
from sqlalchemy.schema import CreateSchema

@dataclass
class Tables:
    settings: Settings

    @cached_property
    def metadata_obj(self):
        return MetaData(schema=self.settings.other.db_schema)

    @cached_property
    def engine(self):
        return create_engine(self.settings.other.path_database)

    @cached_property
    def Session(self):
        return sessionmaker(bind=self.engine)

    @cached_property
    def Base(self):
        return declarative_base(metadata=self.metadata_obj)

    @property
    def Attributes(self):
        return type('Attributes', (object,), {
                k:Column(String)
                for k in self.settings.other.attributes
            }
        )

    @property
    def AttributeComparisons(self):
        return type('AttributeComparisons', (object,), {
                **{
                    f"{k}_l":Column(String)
                    for k in self.settings.other.attributes
                },
                **{
                    f"{k}_r":Column(String)
                    for k in self.settings.other.attributes
                },
            }
        )

    @cached_property        
    def maindf(self):
        return type('df', (self.Attributes, self.Base), {
                "__tablename__":"df",
                "_index":Column(Integer, primary_key=True)
            }
        )

    @cached_property    
    def Sample(self):
        return type('sample', (self.Attributes, self.Base), {
                "__tablename__":"sample",
                "_index":Column(Integer, primary_key=True)
            }
        )

    @cached_property            
    def Pos(self):
        return type('pos', (self.Attributes, self.Base), {
                "__tablename__":"pos",
                "_index":Column(Integer, primary_key=True)
            }
        )


    @cached_property    
    def Neg(self):
        return type('neg', (self.Attributes, self.Base), {
                "__tablename__":"neg",
                "_index":Column(Integer, primary_key=True)
            }
        )

    @cached_property    
    def Train(self):
        return type('train', (self.Attributes, self.Base), {
                "__tablename__":"train",
                "_index":Column(Integer, primary_key=True)
            }
        )

    @cached_property    
    def Labels(self):
        return type('labels', (self.Attributes, self.AttributeComparisons, self.Base), {
                "__tablename__":"labels",
                "_index_l":Column(Integer, primary_key=True),
                "_index_r":Column(Integer, primary_key=True),
                "label":Column(Integer)
            }
        )

    @cached_property    
    def Distances(self):
        return type('distances', (self.Attributes, self.AttributeComparisons, self.Base), {
                "__tablename__":"distances",
                "_index_l":Column(Integer, primary_key=True),
                "_index_r":Column(Integer, primary_key=True),
                "label":Column(Integer)
            }
        )

    @cached_property    
    def FullDistances(self):
        return type('full_distances', (self.Attributes, self.AttributeComparisons, self.Base), {
                "__tablename__":"full_distances",
                "_index_l":Column(Integer, primary_key=True),
                "_index_r":Column(Integer, primary_key=True),
                "label":Column(Integer)
            }
        )

    @cached_property    
    def Comparisons(self):
        return type('comparisons', (self.Base, ), {
                "__tablename__":"comparisons",
                "_index_l":Column(Integer, primary_key=True),
                "_index_r":Column(Integer, primary_key=True)
            }
        )

    @cached_property    
    def FullComparisons(self):
        return type('full_comparisons', (self.Base, ), {
                "__tablename__":"full_comparisons",
                "_index_l":Column(Integer, primary_key=True),
                "_index_r":Column(Integer, primary_key=True)
            }
        )

    @cached_property    
    def Clusters(self):
        return type('clusters', (self.Base, ), {
                "__tablename__":"clusters",
                "_cluster_key":Column(Integer, primary_key=True, autoincrement=True),
                "cluster":Column(Integer),
                "_index":Column(Integer)
            }
        )
    
    def setup_dynamic_declarative_mapping(self):
        """
        see "Declarative Table" in: https://docs.sqlalchemy.org/en/14/orm/declarative_tables.html

        difference here is that we use builtin function type() to generate 
        declarative table dynamically
        """
        return (
            self.maindf, 
            self.Sample, 
            self.Pos, 
            self.Neg,
            self.Train, 
            self.Labels,
            self.Distances,
            self.FullDistances,
            self.Comparisons,
            self.FullComparisons,
            self.Clusters
        )

    def create_schema(self):
        if not self.engine.dialect.has_schema(
            self.engine, 
            self.settings.other.db_schema
        ):
            self.engine.execute(CreateSchema(self.settings.other.db_schema))

    def reset_all_tables(self):
        self.Base.metadata.drop_all(self.engine)
        self.Base.metadata.create_all(self.engine, checkfirst=True)

    def reset_tables(self):
        self.create_schema()
        self.reset_all_tables()