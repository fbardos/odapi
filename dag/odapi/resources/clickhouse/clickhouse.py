import clickhouse_connect
import sqlalchemy
from clickhouse_driver import Client
from dagster import ConfigurableResource


class ClickHouseResource(ConfigurableResource):
    sqlalchemy_connection_string: str

    def get_sqlalchemy_engine(self):
        return sqlalchemy.create_engine(self.sqlalchemy_connection_string)

    def get_clickhouse_client(self) -> Client:
        engine = self.get_sqlalchemy_engine()
        return Client.from_url(engine.url.render_as_string(hide_password=False))
