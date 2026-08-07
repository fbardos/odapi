from pathlib import Path
from typing import Optional

from dagster import ConfigurableResource
from dagster import InitResourceContext
from pydantic import PrivateAttr
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class DuckDBResource(ConfigurableResource):
    database: str
    read_only: bool = False

    _engine: Optional[Engine] = PrivateAttr(default=None)

    def get_sqlalchemy_engine(self) -> Engine:
        if self._engine is None:
            db_path = Path(self.database).as_posix()

            self._engine = create_engine(
                f'duckdb:///{db_path}',
                connect_args={
                    'read_only': self.read_only,
                },
            )

        return self._engine

    def ensure_schema(self, schema: str) -> None:
        engine = self.get_sqlalchemy_engine()
        with engine.begin() as conn:
            conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS {schema}')

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
