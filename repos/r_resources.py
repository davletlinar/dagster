import os
from dagster import resource
from sqlalchemy import Engine
from sqlmodel import create_engine

@resource(
    description="megafon_postgres",
    )
def megafon_postgres() -> Engine:
    host = os.environ['DAGSTER_POSTGRES_HOST']
    port = os.environ['DAGSTER_POSTGRES_PORT']
    dbname = 'megafon'
    user = os.environ['DAGSTER_POSTGRES_USER']
    password = os.environ['DAGSTER_POSTGRES_PASSWORD']
    conn = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    return conn

@resource(
    description="role_postgres",
    )
def role_postgres() -> Engine:
    host = os.environ['DAGSTER_POSTGRES_HOST']
    port = os.environ['DAGSTER_POSTGRES_PORT']
    dbname = os.environ['ROLE_POSTGRES_DB']
    user = os.environ['DAGSTER_POSTGRES_USER']
    password = os.environ['DAGSTER_POSTGRES_PASSWORD']
    conn = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    return conn