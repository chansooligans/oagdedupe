import os

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from oagdedupe.db.postgres.tables import Tables

db_url = os.environ.get("DATABASE_URL")
engine = create_engine(db_url)
engine.connect()

Session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()


@pytest.fixture(scope="module")
def db_session():
    Base.metadata.create_all(engine)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(bind=engine)


def test_connection(db_session):
    res = db_session.query(text("1"))
    assert res.all() == [(1,)]


def test_schema_initialization(settings):
    tables = Tables(settings=settings)
    tables.delete_schema()
    tables.create_schema()
    df = pd.read_sql(
        f"""SELECT schema_name FROM information_schema.schemata
                WHERE schema_name = '{settings.db.db_schema}';""",
        con=tables.engine,
    )
    assert len(df) > 0


def test_schema_inits_tables(settings):
    tables = Tables(settings=settings)
    tables.delete_schema()
    tables.create_schema()
    tables.reset_tables()
    df = pd.read_sql(
        f"""SELECT * FROM information_schema.tables
                WHERE table_schema = '{settings.db.db_schema}';""",
        con=tables.engine,
    )
    assert len(df) == 14
