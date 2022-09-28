import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

try:
    engine = create_engine("postgresql+psycopg2://username:password@0.0.0.0:8000/db")
    engine.connect()
except:
    engine = create_engine("postgres://username:password@postgres:5432/db")
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