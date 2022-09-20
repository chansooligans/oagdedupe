# coding=utf-8

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData

metadata_obj = MetaData(schema="orm")

engine = create_engine('postgresql+psycopg2://username:password@172.22.39.26:8000/db')
Session = sessionmaker(bind=engine)

Base = declarative_base(metadata=metadata_obj)