import requests
import pandas as pd

from dagster import asset, AssetObservation, OpExecutionContext, op, MaterializeResult, MetadataValue, ConfigurableResource, Definitions, EnvVar, Output, load_assets_from_package_module
import sqlalchemy
import psycopg2
from sqlalchemy import Table, Column, String, MetaData, insert
from sqlalchemy.dialects.postgresql import insert


class PostgresResource(ConfigurableResource):
    sqlalchemy_connection_string: str

    def get_sqlalchemy_engine(self):
        return sqlalchemy.create_engine(self.sqlalchemy_connection_string)


class XcomPostgresResource(PostgresResource):
    _XCOM_TABLE = Table(
        'xcom',
        MetaData(schema='public'),
        Column('key', String, primary_key=True),
        Column('value', String),
    )

    def _verify_xcom_table_exists(self):
        engine = self.get_sqlalchemy_engine()
        if not engine.has_table('xcom'):
            self._XCOM_TABLE.create(engine)

    def xcom_pull(self, key):
        self._verify_xcom_table_exists()
        select_stmt = self._XCOM_TABLE.select().where(self._XCOM_TABLE.c.key == key)
        result = self.get_sqlalchemy_engine().execute(select_stmt)
        row = result.fetchone()
        if row:
            return row['value']
        return None

    def xcom_push(self, key, value):
        self._verify_xcom_table_exists()
        insert_stmt = insert(self._XCOM_TABLE).values(key=key, value=value)
        on_duplicate_key_stmt = insert_stmt.on_conflict_do_update(  # upsert
            index_elements=[self._XCOM_TABLE.c.key],
            # set_=dict(value=insert_stmt.values.value),
            set_=dict(value=insert_stmt.excluded.value),
        )
        self.get_sqlalchemy_engine().execute(on_duplicate_key_stmt)

