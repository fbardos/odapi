import datetime as dt
import io
import os
import re
import tempfile
import textwrap
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from pathlib import Path as FilePath
from typing import Optional
from typing import Union

import geopandas as gpd
import networkx as nx
import pandas as pd
import pyarrow.parquet as pq
import pytest
from dotenv import load_dotenv
from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from fastapi import Request
from fastapi import Response
from fastapi import status
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

# from tabulate import tabulate
from geoalchemy2 import Geometry
from shapely import wkb
from sqlalchemy import SMALLINT
from sqlalchemy import TEXT
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import literal
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from starlette.background import BackgroundTask

from odapi.resources.published_models import PublishedModels

# DATABASE ###################################################################
load_dotenv()


def get_sync_engine() -> Engine:
    return create_engine(
        os.environ['SQLALCHEMY_DATABASE_URL_DUCKDB'], connect_args=dict(read_only=True)
    )


def get_metadata():
    return MetaData(schema='dbt')


SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

COMMON_INTERNAL_COLUMNS_VALIDITY = {
    'dbt_valid_from',
    'dbt_valid_to',
}
COMMON_INTERNAL_COLUMNS_FILE_SOURCE = {
    # TODO: Add ckan source (ID + URL)
    'file_source',
}

COMMON_INTERNAL_COLUMNS_GEOM = {
    'geom',
    'lat',
    'lon',
}


def build_select_columns(
    engine: Engine,
    table_name: str,
    show_validity: bool,
    show_file_source: bool,
    show_geometry: bool,
    requested_columns: Optional[list[str]] = None,
    exclude_columns: Optional[list[str]] = None,
    schema_name: str = 'dbt_marts',
) -> str:
    inspector = inspect(engine)
    table_columns = inspector.get_columns(table_name, schema=schema_name)
    existing_column_names = [col['name'] for col in table_columns]
    existing_column_set = set(existing_column_names)
    if requested_columns:
        unknown_columns = set(requested_columns) - existing_column_set
        if unknown_columns:
            raise HTTPException(
                status_code=400,
                detail={
                    'message': 'Unknown requested columns',
                    'columns': sorted(unknown_columns),
                },
            )

        selected_columns = requested_columns
    else:
        selected_columns = existing_column_names

    if exclude_columns:
        exclude_set = set(exclude_columns)
    else:
        exclude_set = set()

    unknown_excluded_columns = exclude_set - existing_column_set
    if unknown_excluded_columns:
        raise HTTPException(
            status_code=400,
            detail={
                'message': 'Unknown excluded columns',
                'columns': sorted(unknown_excluded_columns),
            },
        )

    if not show_validity:
        exclude_set |= COMMON_INTERNAL_COLUMNS_VALIDITY
    if not show_file_source:
        exclude_set |= COMMON_INTERNAL_COLUMNS_FILE_SOURCE
    if not show_geometry:
        exclude_set |= COMMON_INTERNAL_COLUMNS_GEOM

    selected_columns = [
        column for column in selected_columns if column not in exclude_set
    ]

    if not selected_columns:
        raise HTTPException(
            status_code=400,
            detail='No columns selected',
        )

    preparer = engine.dialect.identifier_preparer

    return ', '.join(preparer.quote(column) for column in selected_columns)


# CUSTOM CLASSES #############################################################
class GeoCode(str, Enum):
    polg = 'polg'
    bezk = 'bezk'
    kant = 'kant'


class GeometryMode(str, Enum):
    point = 'point'
    border = 'border'
    border_simple_50m = 'border_simple_50_meter'
    border_simple_100m = 'border_simple_100_meter'
    border_simple_500m = 'border_simple_500_meter'


class Measure(str, Enum):
    zahl = 'zahl'
    pro100 = 'pro100'
    pro1000 = 'pro1000'
    pro100000 = 'pro100000'


class GeoJsonResponse(Response):
    media_type = 'application/geo+json'


class CsvResponse(Response):
    media_type = 'text/csv'

    def __init__(
        self,
        content: io.BytesIO,
        filename: str = 'odapi_data.csv',
        status_code: int = 200,
        *args,
        **kwargs,
    ):
        super().__init__(
            content=content.getvalue(),
            status_code=status_code,
            headers={'Content-Disposition': f'attachment; filename={filename}'},
            media_type=self.media_type,
            *args,
            **kwargs,
        )


class XlsxResponse(Response):
    media_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

    def __init__(
        self,
        content: io.BytesIO,
        filename: str = 'odapi_data.xlsx',
        status_code: int = 200,
        *args,
        **kwargs,
    ):
        super().__init__(
            content=content.getvalue(),
            status_code=status_code,
            headers={'Content-Disposition': f'attachment; filename={filename}'},
            media_type=self.media_type,
            *args,
            **kwargs,
        )


class GeoparquetResponse(Response):
    # media_type = 'application/x-parquet'
    media_type = 'application/octet-stream'

    def __init__(
        self,
        content: io.BytesIO,
        filename: str = 'odapi_data.parquet',
        status_code: int = 200,
        *args,
        **kwargs,
    ):
        super().__init__(
            content=content.getvalue(),
            status_code=status_code,
            headers={'Content-Disposition': f'attachment; filename={filename}'},
            media_type=self.media_type,
            *args,
            **kwargs,
        )


class TxtResponose(Response):
    media_type = 'text/plain'

    # def __init__(self, content: str, filename: str = 'odapi_data.txt', status_code: int = 200, *args, **kwargs):
    def __init__(self, content: str, status_code: int = 200, *args, **kwargs):
        super().__init__(
            content=content,
            status_code=status_code,
            # headers={'Content-Disposition': f'attachment; filename={filename}'},
            media_type=self.media_type,
            *args,
            **kwargs,
        )


# API ########################################################################
app = FastAPI(
    title='ODAPI - Open Data API',
    docs_url='/',
    summary='Merged data for different cantons and municipalities in Switzerland.',
    description=textwrap.dedent("""
        ## IMPORTANT

        * This API is under heavy development. Endpoints and responses **can and will change in the future**.
        * This is **not** an official API from the Swiss Government, but just a private iniative from [myself](https://bardos.dev).

        ## Reference

        * [Documentation](https://odapi.bardos.dev/docs).
        * Source code [on Github](https://github.com/fbardos/odapi).
    """),
)


def get_response_class(request: Request):
    route = request.scope.get('route')
    if isinstance(route, APIRoute):
        return route.response_class
    return None


# MODELS #####################################################################
@app.get(
    '/models/tsv',
    tags=['Models'],
    summary='Get all available models (TSV)',
    response_class=TxtResponose,
)
@app.get(
    '/models',
    tags=['Models'],
    summary='Get all available models (Default, JSON)',
    response_class=JSONResponse,
)
def get_models(request: Request):
    model_resources = PublishedModels().model_resources
    match get_response_class(request):
        case cls if cls is TxtResponose:
            df = pd.DataFrame(model_resources)
            df = df[['name', 'publisher_type']]
            buffer = io.StringIO()
            df.to_csv(buffer, sep="\t", index=False, encoding="utf-8")
            buffer.seek(0)
            return StreamingResponse(
                iter([buffer.getvalue()]),
                media_type='text/csv',
            )
        case _:
            return model_resources


# MODEL ######################################################################
# TODO: JOIN publisher name, url, geom_code, kanton/gemeinde_bfs_id
# TODO: JOIN kanton (on data)
# TODO: JOIN gemeinde (on data)
# TODO: JOIN country (on data)
# TODO: JOIN license
@app.get(
    '/model/{model_name}/xlsx',
    tags=['Model'],
    summary='Get Model (XLSX)',
    response_class=XlsxResponse,
)
@app.get(
    '/model/{model_name}/parquet',
    tags=['Model'],
    summary='Get Model (Parquet)',
    response_class=GeoparquetResponse,
)
@app.get(
    '/model/{model_name}/tsv',
    tags=['Model'],
    summary='Get Model (TSV)',
    response_class=TxtResponose,
)
@app.get(
    '/model/{model_name}',
    tags=['Model'],
    summary='Get Model (Default, CSV)',
    response_class=CsvResponse,
)
def get_model(
    model_name: str,
    request: Request,
    db_sync: Engine = Depends(get_sync_engine),
    limit: Optional[int] = Query(None, ge=1),
    offset: int = Query(0, ge=0),
    knowledge_date: Optional[dt.date] = Query(
        None,
        examples=[dt.date.today().strftime('%Y-%m-%d')],
        description='Optional. Allows to query a different state of the data in the past. Format: ISO-8601',
    ),
    show_validity: bool = Query(False),
    show_file_source: bool = Query(False),
    show_geometry: bool = Query(False),
):

    def quote_duckdb_string(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    schema_name = 'dbt_marts'
    if not SAFE_IDENTIFIER.fullmatch(model_name):
        raise HTTPException(status_code=400, detail='Invalid model_name')

    try:
        # Make sure, table exists
        inspector = inspect(db_sync)
        if not inspector.has_table(model_name, schema=schema_name):
            raise HTTPException(status_code=404, detail='Model/table not found')

        # quote identifiers safely
        preparer = db_sync.dialect.identifier_preparer
        quoted_schema = preparer.quote_schema(schema_name)
        quoted_table = preparer.quote(model_name)

        params = {'offset': offset}

        where_clauses = []
        limit_clause = ''
        if limit is not None:
            limit_clause = 'LIMIT :limit'
            params['limit'] = limit
        if knowledge_date is not None:
            where_clauses.append("""
                :knowledge_date BETWEEN dbt_valid_from
                AND COALESCE(dbt_valid_to, 'infinity'::timestamptz)
            """)
            params['knowledge_date'] = knowledge_date

        where_sql = ''
        if where_clauses:
            where_sql = 'WHERE ' + ' AND '.join(where_clauses)

        selected_columns_sql = build_select_columns(
            engine=db_sync.engine,
            table_name=quoted_table,
            show_validity=show_validity,
            show_file_source=show_file_source,
            show_geometry=show_geometry,
        )
        sql = text(f'''
            SELECT {selected_columns_sql}
            FROM {quoted_schema}.{quoted_table}
            {where_sql}
            {limit_clause}
            OFFSET :offset
        ''')

        try:
            with db_sync.connect() as conn:
                compiled_query = sql.bindparams(**params).compile(
                    dialect=db_sync.dialect,
                    compile_kwargs={'literal_binds': True},
                )

                response_class = get_response_class(request)

                match response_class:
                    case cls if cls is TxtResponose:
                        suffix = '.tsv'
                        media_type = 'text/tab-separated-values'
                        filename = 'export.tsv'
                        copy_options = '''
                            FORMAT CSV,
                            DELIMITER '\t',
                            HEADER TRUE,
                            NULLSTR ''
                        '''

                    case cls if cls is CsvResponse:
                        suffix = '.csv'
                        media_type = 'text/csv'
                        filename = 'export.csv'
                        copy_options = '''
                            FORMAT CSV,
                            HEADER TRUE
                        '''

                    case _:
                        suffix = '.parquet'
                        media_type = 'application/vnd.apache.parquet'
                        filename = 'export.parquet'
                        copy_options = '''
                            FORMAT PARQUET
                        '''

                fd, output_path = tempfile.mkstemp(
                    prefix='duckdb_export_',
                    suffix=suffix,
                )
                os.close(fd)

                quoted_output_path = quote_duckdb_string(output_path)

                copy_sql = f'''
                    COPY (
                        {compiled_query}
                    )
                    TO {quoted_output_path}
                    WITH (
                        {copy_options}
                    )
                '''

                try:
                    raw_connection = conn.connection.driver_connection
                    raw_connection.execute('LOAD spatial')
                    raw_connection.execute(copy_sql)

                    return FileResponse(
                        path=output_path,
                        media_type=media_type,
                        filename=filename,
                        background=BackgroundTask(
                            lambda: FilePath(output_path).unlink(missing_ok=True)
                        ),
                    )

                except Exception:
                    FilePath(output_path).unlink(missing_ok=True)
                    raise
        finally:
            db_sync.dispose()

    except HTTPException:
        raise
    except SQLAlchemyError as err:
        raise HTTPException(status_code=500, detail='Database error') from err
