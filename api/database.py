import os
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from sqlalchemy import Table


envs = load_dotenv()

engine = create_engine(os.environ['SQLALCHEMY_DATABASE_URL'])
metadata = MetaData(bind=None, schema='dbt')


def get_db():
    return engine, metadata


def get_db_marts():
    metadata_marts = MetaData(bind=None, schema='dbt_marts')
    return engine, metadata_marts


def get_session():
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


table__mart_ogd_api = Table('mart_ogd_api', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=engine)
table__seed_indicators = Table('seed_indicator', MetaData(bind=None, schema='dbt'), autoload=True, autoload_with=engine)
