from .base import Base
from sqlalchemy import Column, String, Integer, Date


attributes = {
    "givenname":str, 
    "surname":str, 
    "suburb":str, 
    "postcode":str
}

# maindf = type('df', (Base,), {
#         **{
#             "__tablename__":"df",
#             "_index":Column(Integer, primary_key=True, autoincrement=True)
#         },
#         **{
#             k:Column(String)
#             for k,v in attributes.items()
#         }
#     }
# )

Attributes = type('Attributes', (object,), {
        k:Column(String)
        for k,v in attributes.items()
    }
)

AttributeComparisons = type('AttributeComparisons', (object,), {
        **{
            f"{k}_l":Column(String)
            for k,v in attributes.items()
        },
        **{
            f"{k}_r":Column(String)
            for k,v in attributes.items()
        },
    }
)

class maindf(Attributes, Base):
    __tablename__ = "df"
    _index = Column(Integer, primary_key=True, autoincrement=True)

class Sample(Attributes, Base):
    __tablename__ = "sample"
    _index = Column(Integer, primary_key=True, autoincrement=True)

class Pos(Attributes, Base):
    __tablename__ = "pos"
    _pos_key = Column(Integer, primary_key=True, autoincrement=True)
    _index = Column(Integer)

class Neg(Attributes, Base):
    __tablename__ = "neg"
    _index = Column(Integer, primary_key=True, autoincrement=True)

class Train(Attributes, Base):
    __tablename__ = "train"
    _train_key = Column(Integer, primary_key=True, autoincrement=True)
    _index = Column(Integer)

class Labels(Attributes, AttributeComparisons, Base):
    __tablename__ = "labels"
    _label_key = Column(Integer, primary_key=True, autoincrement=True)
    _index_l = Column(Integer)
    _index_r = Column(Integer)