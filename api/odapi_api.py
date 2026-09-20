import datetime as dt
import io
import os
import re
import textwrap
from collections.abc import Iterator
from enum import Enum
from typing import Optional
from typing import cast

import pandas as pd
from clickhouse_connect.dbapi.connection import Connection as ClickHouseDbapiConnection
from dotenv import load_dotenv
from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import Response
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRoute
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# DATABASE ###################################################################
load_dotenv()


DATABASE_NAME = 'data_marts'


SYNC_ENGINE = create_engine(
    os.environ['SQLALCHEMY_DATABASE_URL_CLICKHOUSE'],
)


def get_sync_engine() -> Engine:
    return SYNC_ENGINE


def get_metadata() -> MetaData:
    return MetaData(schema=DATABASE_NAME)


SAFE_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


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
    schema_name: str = DATABASE_NAME,
) -> str:
    inspector = inspect(engine)

    table_columns = inspector.get_columns(
        table_name,
        schema=schema_name,
    )

    existing_column_names = [column['name'] for column in table_columns]

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

    exclude_set = set(exclude_columns or [])

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


def stream_clickhouse_query(
    engine: Engine,
    sql: str,
    parameters: dict[str, object],
    output_format: str,
) -> Iterator[bytes]:
    with engine.connect() as connection:
        dbapi_connection = cast(
            ClickHouseDbapiConnection,
            connection.connection.driver_connection,
        )

        stream = dbapi_connection.client.raw_stream(
            query=sql,
            parameters=parameters,
            settings={
                'format_csv_null_representation': '',
            },
            fmt=output_format,
        )

        try:
            while True:
                chunk = stream.read(1024 * 1024)

                if not chunk:
                    break

                yield chunk

        finally:
            stream.close()


def query_clickhouse_dataframe(
    engine: Engine,
    sql: str,
    parameters: dict[str, object],
) -> pd.DataFrame:
    with engine.connect() as connection:
        dbapi_connection = cast(
            ClickHouseDbapiConnection,
            connection.connection.driver_connection,
        )

        return dbapi_connection.client.query_df(
            query=sql,
            parameters=parameters,
        )


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
        *args: object,
        **kwargs: object,
    ) -> None:
        super().__init__(
            content=content.getvalue(),
            status_code=status_code,
            headers={
                'Content-Disposition': f'attachment; filename={filename}',
            },
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
        *args: object,
        **kwargs: object,
    ) -> None:
        super().__init__(
            content=content.getvalue(),
            status_code=status_code,
            headers={
                'Content-Disposition': f'attachment; filename={filename}',
            },
            media_type=self.media_type,
            *args,
            **kwargs,
        )


class GeoparquetResponse(Response):
    media_type = 'application/vnd.apache.parquet'

    def __init__(
        self,
        content: io.BytesIO,
        filename: str = 'odapi_data.parquet',
        status_code: int = 200,
        *args: object,
        **kwargs: object,
    ) -> None:
        super().__init__(
            content=content.getvalue(),
            status_code=status_code,
            headers={
                'Content-Disposition': f'attachment; filename={filename}',
            },
            media_type=self.media_type,
            *args,
            **kwargs,
        )


class TxtResponose(Response):
    media_type = 'text/plain'

    def __init__(
        self,
        content: str,
        status_code: int = 200,
        *args: object,
        **kwargs: object,
    ) -> None:
        super().__init__(
            content=content,
            status_code=status_code,
            media_type=self.media_type,
            *args,
            **kwargs,
        )


# API ########################################################################
app = FastAPI(
    title='ODAPI - Open Data API',
    docs_url='/',
    summary='Merged data for different cantons and municipalities in Switzerland.',
    description=textwrap.dedent('''
        ## IMPORTANT

        * This API is under heavy development. Endpoints and responses **can and will change in the future**.
        * This is **not** an official API from the Swiss Government, but just a private iniative from [myself](https://bardos.dev).

        ## Reference

        * [Documentation](https://odapi.bardos.dev/docs).
        * Source code [on Github](https://github.com/fbardos/odapi).
        '''),
)


def get_response_class(
    request: Request,
) -> Optional[type[Response]]:
    route = request.scope.get('route')

    if isinstance(route, APIRoute):
        return route.response_class

    return None


# # MODELS ###################################################################
# @app.get(
#     '/models/tsv',
#     tags=['Models'],
#     summary='Get all available models (TSV)',
#     response_class=TxtResponose,
# )
# @app.get(
#     '/models',
#     tags=['Models'],
#     summary='Get all available models (Default, JSON)',
#     response_class=JSONResponse,
# )
# def get_models(request: Request) -> object:
#     model_resources = PublishedModels().model_resources
#
#     match get_response_class(request):
#         case cls if cls is TxtResponose:
#             df = pd.DataFrame(model_resources)
#             df = df[['name', 'publisher_type']]
#
#             buffer = io.StringIO()
#
#             df.to_csv(
#                 buffer,
#                 sep='\t',
#                 index=False,
#                 encoding='utf-8',
#             )
#
#             buffer.seek(0)
#
#             return StreamingResponse(
#                 iter([buffer.getvalue()]),
#                 media_type='text/csv',
#             )
#
#         case _:
#             return model_resources


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
    limit: Optional[int] = Query(
        None,
        ge=1,
    ),
    offset: int = Query(
        0,
        ge=0,
    ),
    knowledge_date: Optional[dt.date] = Query(
        None,
        examples=[
            dt.date.today().strftime('%Y-%m-%d'),
        ],
        description=(
            'Optional. Allows to query a different state of the data '
            'in the past. Format: ISO-8601'
        ),
    ),
    show_validity: bool = Query(False),
    show_file_source: bool = Query(False),
    show_geometry: bool = Query(False),
) -> Response:
    schema_name = DATABASE_NAME

    if not SAFE_IDENTIFIER.fullmatch(model_name):
        raise HTTPException(
            status_code=400,
            detail='Invalid model_name',
        )

    try:
        inspector = inspect(db_sync)

        if not inspector.has_table(
            model_name,
            schema=schema_name,
        ):
            raise HTTPException(
                status_code=404,
                detail='Model/table not found',
            )

        preparer = db_sync.dialect.identifier_preparer

        quoted_schema = preparer.quote_schema(schema_name)
        quoted_table = preparer.quote(model_name)

        selected_columns_sql = build_select_columns(
            engine=db_sync,
            table_name=model_name,
            show_validity=show_validity,
            show_file_source=show_file_source,
            show_geometry=show_geometry,
            schema_name=schema_name,
        )

        parameters: dict[str, object] = {
            'offset': offset,
        }

        where_clauses: list[str] = []

        if knowledge_date is not None:
            where_clauses.append('''
                dbt_valid_from <= toDateTime({knowledge_date:Date})
                AND (
                    dbt_valid_to IS NULL
                    OR toDateTime({knowledge_date:Date}) <= dbt_valid_to
                )
                ''')

            parameters['knowledge_date'] = knowledge_date

        where_sql = ''

        if where_clauses:
            where_sql = 'WHERE ' + ' AND '.join(where_clauses)

        if limit is not None:
            limit_sql = '''
                LIMIT {limit:UInt64}
                OFFSET {offset:UInt64}
            '''

            parameters['limit'] = limit

        elif offset > 0:
            limit_sql = '''
                LIMIT 18446744073709551615
                OFFSET {offset:UInt64}
            '''

        else:
            limit_sql = ''

        sql = f'''
            SELECT
                {selected_columns_sql}
            FROM {quoted_schema}.{quoted_table}
            {where_sql}
            {limit_sql}
        '''

        response_class = get_response_class(request)

        if response_class is TxtResponose:
            return StreamingResponse(
                stream_clickhouse_query(
                    engine=db_sync,
                    sql=sql,
                    parameters=parameters,
                    output_format='TabSeparatedWithNames',
                ),
                media_type='text/tab-separated-values',
                headers={
                    'Content-Disposition': ('attachment; filename=export.tsv'),
                },
            )

        if response_class is CsvResponse:
            return StreamingResponse(
                stream_clickhouse_query(
                    engine=db_sync,
                    sql=sql,
                    parameters=parameters,
                    output_format='CSVWithNames',
                ),
                media_type='text/csv',
                headers={
                    'Content-Disposition': ('attachment; filename=export.csv'),
                },
            )

        if response_class is GeoparquetResponse:
            return StreamingResponse(
                stream_clickhouse_query(
                    engine=db_sync,
                    sql=sql,
                    parameters=parameters,
                    output_format='Parquet',
                ),
                media_type='application/vnd.apache.parquet',
                headers={
                    'Content-Disposition': ('attachment; filename=export.parquet'),
                },
            )

        if response_class is XlsxResponse:
            dataframe = query_clickhouse_dataframe(
                engine=db_sync,
                sql=sql,
                parameters=parameters,
            )

            buffer = io.BytesIO()

            dataframe.to_excel(
                buffer,
                index=False,
            )

            buffer.seek(0)

            return XlsxResponse(
                content=buffer,
                filename='export.xlsx',
            )

        raise HTTPException(
            status_code=500,
            detail='Unsupported response format',
        )

    except HTTPException:
        raise

    except SQLAlchemyError as err:
        raise HTTPException(
            status_code=500,
            detail='Database error',
        ) from err
