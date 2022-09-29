import os

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from oagdedupe.db.tables import Tables
from oagdedupe.settings import Settings, SettingsOther

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


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings(
        name="default",  # the name of the project, a unique identifier
        folder="./.dedupe_test",  # path to folder where settings and data will be saved
        other=SettingsOther(
            dedupe=False,
            n=5000,
            k=3,
            max_compare=20_000,
            n_covered=5_000,
            cpus=20,  # parallelize distance computations
            attributes=["name", "addr"],  # list of entity attribute names
            path_database=os.environ.get(
                "DATABASE_URL"
            ),  # where to save the sqlite database holding intermediate data
            db_schema="dedupe",
            path_model="./.dedupe_test/test_model",  # where to save the model
            label_studio={
                "port": 8089,  # label studio port
                "api_key": "33344e8a477f8adc3eb6aa1e41444bde76285d96",  # label studio port
                "description": "chansoo test project",  # label studio description of project
            },
            fast_api={"port": 8090},  # fast api port
        ),
    )


def test_connection(db_session):
    res = db_session.query(text("1"))
    assert res.all() == [(1,)]


def test_schema_initialization(settings):
    tables = Tables(settings=settings)
    tables.delete_schema()
    tables.create_schema()
    df = pd.read_sql(
        f"""SELECT schema_name FROM information_schema.schemata
                WHERE schema_name = '{settings.other.db_schema}';""",
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
                WHERE table_schema = '{settings.other.db_schema}';""",
        con=tables.engine,
    )
    assert len(df) == 17
