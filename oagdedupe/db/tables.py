""" this module is used to initialize sqlalchmey tables using orm
"""

from oagdedupe import utils as du
from oagdedupe.settings import Settings

from dataclasses import dataclass
from functools import cached_property
from sqlalchemy import Column, String, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, create_engine
from sqlalchemy.schema import CreateSchema

class TablesRecordLinkage:
    """ contains tables used only for recordl inkage
    """

    @cached_property        
    def maindf_link(self):
        """table for df_link"""
        return type('df_link', (self.BaseAttributes, self.Base), {
                "__tablename__":"df_link",
                "_index":Column(Integer, primary_key=True)
            }
        )

    @cached_property    
    def Neg_link(self):
        """table for neg_link"""
        return type('neg_link', (self.BaseAttributes, self.Base), {
                "__tablename__":"neg_link",
                "_index":Column(Integer, primary_key=True),
                "labelled":Column(Boolean)
            }
        )

    @cached_property    
    def Unlabelled_link(self):
        """table for unlabelled_link"""
        return type('unlabelled_link', (self.BaseAttributes, self.Base), {
                "__tablename__":"unlabelled_link",
                "_index":Column(Integer, primary_key=True),
                "labelled":Column(Boolean)
            }
        )

    @cached_property    
    def Train_link(self):
        """table for train_link"""
        return type('train_link', (self.BaseAttributes, self.Base), {
                "__tablename__":"train_link",
                "_index":Column(Integer, primary_key=True),
                "labelled":Column(Boolean)
            }
        )


@dataclass
class Tables(TablesRecordLinkage):
    """
    Factory to create sql alchemy "Declarative Table" 
    (https://docs.sqlalchemy.org/en/14/orm/declarative_tables.html)

    We use builtin function type() to generate declarative table dynamically
    since attribute names are variable
    """
    settings: Settings

    def setup_dynamic_declarative_mapping(self):
        """ initializes declarative tables
        """
        return (
            self.maindf, 
            self.Pos, 
            self.Neg,
            self.Unlabelled,
            self.Train, 

            self.maindf_link, 
            self.Neg_link,
            self.Unlabelled_link,
            self.Train_link, 

            self.Labels,
            self.LabelsDistances,
            self.Distances,
            self.FullDistances,
            self.Comparisons,
            self.FullComparisons,
            self.Clusters
        )

    @cached_property
    def metadata_obj(self):
        """ A sqlalchemy MetaData collection that stores Tables
        """
        return MetaData(schema=self.settings.other.db_schema)

    @cached_property
    def Base(self):
        """ All Table objects declared by subclasses of this Base share a 
        common metadata; that is they are all part of the same 
        Metadata collection
        """
        return declarative_base(metadata=self.metadata_obj)

    @cached_property
    def engine(self):
        """manages dbapi connection, created once"""
        return create_engine(self.settings.other.path_database)

    @cached_property
    def Session(self):
        """creates connection resource from engine when queries issued;
        it should be used with a context manager

        Example
        ----------
        with Session(engine) as session:
            session.add(some_object)
            session.add(some_other_object)
            session.commit()
        """
        return sessionmaker(bind=self.engine)

    @property
    def BaseAttributes(self):
        """mixin table used to share attribute columns"""
        return type('Attributes', (object,), {
                k:Column(String)
                for k in self.settings.other.attributes
            }
        )

    @property
    def BaseAttributeComparisons(self):
        """mixin table used to share attribute comparison columns"""
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
        """table for df"""
        return type('df', (self.BaseAttributes, self.Base), {
                "__tablename__":"df",
                "_index":Column(Integer, primary_key=True)
            }
        )

    @cached_property            
    def Pos(self):
        """table for pos"""
        return type('pos', (self.BaseAttributes, self.Base), {
                "__tablename__":"pos",
                "_index":Column(Integer, primary_key=True),
                "labelled":Column(Boolean)
            }
        )

    @cached_property    
    def Neg(self):
        """table for neg"""
        return type('neg', (self.BaseAttributes, self.Base), {
                "__tablename__":"neg",
                "_index":Column(Integer, primary_key=True),
                "labelled":Column(Boolean)
            }
        )


    @cached_property    
    def Unlabelled(self):
        """table for unlabelled"""
        return type('unlabelled', (self.BaseAttributes, self.Base), {
                "__tablename__":"unlabelled",
                "_index":Column(Integer, primary_key=True),
                "labelled":Column(Boolean)
            }
        )

    @cached_property    
    def Train(self):
        """table for train"""
        return type('train', (self.BaseAttributes, self.Base), {
                "__tablename__":"train",
                "_index":Column(Integer, primary_key=True),
                "labelled":Column(Boolean)
            }
        )

    @cached_property    
    def Labels(self):
        """table for labels"""
        return type('labels', (self.Base, ), {
                "__tablename__":"labels",
                "_index_l":Column(Integer, primary_key=True),
                "_index_r":Column(Integer, primary_key=True),
                "label":Column(Integer)
            }
        )

    @cached_property    
    def LabelsDistances(self):
        """table for labels_distances"""
        return type('labels_distances', (self.BaseAttributes, self.BaseAttributeComparisons, self.Base), {
                "__tablename__":"labels_distances",
                "_index_l":Column(Integer, primary_key=True),
                "_index_r":Column(Integer, primary_key=True),
                "label":Column(Integer)
            }
        )

    @cached_property    
    def Distances(self):
        """table for distances"""
        return type('distances', (self.BaseAttributes, self.BaseAttributeComparisons, self.Base), {
                "__tablename__":"distances",
                "_index_l":Column(Integer, primary_key=True),
                "_index_r":Column(Integer, primary_key=True),
                "label":Column(Integer)
            }
        )

    @cached_property    
    def FullDistances(self):
        """table for full_distances"""
        return type('full_distances', (self.BaseAttributes, self.BaseAttributeComparisons, self.Base), {
                "__tablename__":"full_distances",
                "_index_l":Column(Integer, primary_key=True),
                "_index_r":Column(Integer, primary_key=True),
                "label":Column(Integer)
            }
        )

    @cached_property    
    def Comparisons(self):
        """table for comparisons"""
        return type('comparisons', (self.Base, ), {
                "__tablename__":"comparisons",
                "_index_l":Column(Integer, primary_key=True),
                "_index_r":Column(Integer, primary_key=True),
                "label":Column(Integer)
            }
        )

    @cached_property    
    def FullComparisons(self):
        """table for full_comparisons"""
        return type('full_comparisons', (self.Base, ), {
                "__tablename__":"full_comparisons",
                "_index_l":Column(Integer, primary_key=True),
                "_index_r":Column(Integer, primary_key=True),
                "label":Column(Integer)
            }
        )

    @cached_property    
    def Clusters(self):
        """table for clusters"""
        return type('clusters', (self.Base, ), {
                "__tablename__":"clusters",
                "_cluster_key":Column(Integer, primary_key=True, autoincrement=True),
                "cluster":Column(Integer),
                "_type":Column(Boolean),
                "_index":Column(Integer)
            }
        )

    def create_schema(self):
        """helper function to create a schema using sqlalchemy orm
        """
        if not self.engine.dialect.has_schema(
            self.engine, 
            self.settings.other.db_schema
        ):
            self.engine.execute(CreateSchema(self.settings.other.db_schema))

    def reset_all_tables(self):
        """deletes all tables and creates all tables
        """
        self.Base.metadata.drop_all(self.engine)
        self.Base.metadata.create_all(self.engine, checkfirst=True)

    def reset_tables(self):
        """resets all tables
        """
        self.create_schema()
        self.reset_all_tables()