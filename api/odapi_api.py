import datetime as dt
import io
import os
import re
import textwrap
from collections import deque
from collections.abc import Iterator
from enum import Enum
from typing import BinaryIO
from typing import Literal
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
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from thrift.protocol import TCompactProtocol
from thrift.Thrift import TType
from thrift.transport import TTransport

# DATABASE ###################################################################
load_dotenv()


DATABASE_NAME = 'data_marts'


XLSX_MAX_ROWS = 1_048_576
XLSX_MAX_DATA_ROWS = XLSX_MAX_ROWS - 1  # one row is used by the header
XLSX_MAX_COLUMNS = 16_384


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
    'geometry',
    'geom',
    'lat',
    'lon',
}


CLICKHOUSE_GEOMETRY_TYPES = {
    'Geometry',
    'Point',
    'Ring',
    'LineString',
    'MultiLineString',
    'Polygon',
    'MultiPolygon',
}


# ClickHouse 26.8 can write these top-level, non-nullable geo types
# directly as valid GeoParquet (WKB payload + `geo` footer metadata).
CLICKHOUSE_GEOPARQUET_TYPES = {
    'Point',
    'LineString',
    'MultiLineString',
    'Polygon',
    'MultiPolygon',
}


GeometryEncoding = Literal['native', 'wkt', 'wkb']


def get_clickhouse_table_columns(
    engine: Engine,
    table_name: str,
    schema_name: str = DATABASE_NAME,
) -> dict[str, str]:
    with engine.connect() as connection:
        dbapi_connection = cast(
            ClickHouseDbapiConnection,
            connection.connection.driver_connection,
        )

        result = dbapi_connection.client.query(
            query='''
                SELECT
                    name,
                    type
                FROM system.columns
                WHERE database = {schema_name:String}
                  AND table = {table_name:String}
                ORDER BY position
            ''',
            parameters={
                'schema_name': schema_name,
                'table_name': table_name,
            },
        )

    return {
        cast(str, name): cast(str, type_name)
        for name, type_name in result.result_rows
    }


def unwrap_clickhouse_type(type_name: str) -> str:
    for wrapper in ('Nullable', 'LowCardinality'):
        prefix = f'{wrapper}('

        if type_name.startswith(prefix) and type_name.endswith(')'):
            return unwrap_clickhouse_type(type_name[len(prefix):-1])

    return type_name


def is_clickhouse_geometry_type(type_name: str) -> bool:
    return unwrap_clickhouse_type(type_name) in CLICKHOUSE_GEOMETRY_TYPES


def is_nullable_clickhouse_type(type_name: str) -> bool:
    type_name = type_name.strip()

    if type_name.startswith('Nullable(') and type_name.endswith(')'):
        return True

    low_cardinality_prefix = 'LowCardinality('

    if (
        type_name.startswith(low_cardinality_prefix)
        and type_name.endswith(')')
    ):
        return is_nullable_clickhouse_type(
            type_name[len(low_cardinality_prefix):-1]
        )

    return False


def is_clickhouse_string_type(type_name: str) -> bool:
    base_type = unwrap_clickhouse_type(type_name)
    return base_type == 'String'


def build_select_columns(
    engine: Engine,
    table_columns: dict[str, str],
    show_validity: bool,
    show_file_source: bool,
    show_geometry: bool,
    requested_columns: Optional[list[str]] = None,
    exclude_columns: Optional[list[str]] = None,
    geometry_encoding: GeometryEncoding = 'native',
    geometry_output_name: Optional[str] = None,
) -> tuple[str, list[str], list[str]]:
    existing_column_names = list(table_columns)
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

    geometry_columns = {
        name
        for name, type_name in table_columns.items()
        if is_clickhouse_geometry_type(type_name)
    }

    if not show_validity:
        exclude_set |= COMMON_INTERNAL_COLUMNS_VALIDITY

    if not show_file_source:
        exclude_set |= COMMON_INTERNAL_COLUMNS_FILE_SOURCE

    if not show_geometry:
        exclude_set |= COMMON_INTERNAL_COLUMNS_GEOM
        exclude_set |= geometry_columns

    selected_columns = [
        column for column in selected_columns if column not in exclude_set
    ]

    if not selected_columns:
        raise HTTPException(
            status_code=400,
            detail='No columns selected',
        )

    selected_geometry_columns = [
        column for column in selected_columns if column in geometry_columns
    ]

    if (
        geometry_output_name
        and selected_geometry_columns
        and geometry_output_name in selected_columns
        and geometry_output_name not in selected_geometry_columns
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                f'Cannot export geometry as {geometry_output_name!r}: '
                'a non-geometry column already uses that name'
            ),
        )

    preparer = engine.dialect.identifier_preparer
    expressions: list[str] = []

    for column in selected_columns:
        quoted_column = preparer.quote(column)

        if column not in geometry_columns or geometry_encoding == 'native':
            expressions.append(quoted_column)
            continue

        output_name = geometry_output_name or column
        quoted_output_name = preparer.quote(output_name)

        if geometry_encoding == 'wkt':
            expressions.append(
                f'wkt({quoted_column}) AS {quoted_output_name}'
            )
            continue

        if geometry_encoding == 'wkb':
            expressions.append(
                f'wkb({quoted_column}) AS {quoted_output_name}'
            )
            continue


    return ', '.join(expressions), selected_geometry_columns, selected_columns


def stream_clickhouse_query(
    engine: Engine,
    sql: str,
    parameters: dict[str, object],
    output_format: str,
    settings: Optional[dict[str, object]] = None,
) -> Iterator[bytes]:
    with engine.connect() as connection:
        dbapi_connection = cast(
            ClickHouseDbapiConnection,
            connection.connection.driver_connection,
        )

        stream = cast(
            BinaryIO,
            dbapi_connection.client.raw_stream(
                query=sql,
                parameters=parameters,
                settings=settings,
                fmt=output_format,
            ),
        )

        try:
            while chunk := stream.read(1024 * 1024):
                yield chunk

        finally:
            stream.close()



_STREAM_CHUNK_SIZE = 1024 * 1024
_PARQUET_TAIL_SIZE = 16 * 1024 * 1024
_PARQUET_MAGIC = b'PAR1'
_PARQUET_BYTE_ARRAY = 6
_PARQUET_CONVERTED_TYPE_UTF8 = 0


ThriftStruct = list[tuple[int, int, object]]


def _read_thrift_value(
    protocol: TCompactProtocol.TCompactProtocol,
    type_id: int,
) -> object:
    if type_id == TType.BOOL:
        return protocol.readBool()

    if type_id == TType.BYTE:
        return protocol.readByte()

    if type_id == TType.I16:
        return protocol.readI16()

    if type_id == TType.I32:
        return protocol.readI32()

    if type_id == TType.I64:
        return protocol.readI64()

    if type_id == TType.DOUBLE:
        return protocol.readDouble()

    if type_id == TType.STRING:
        return protocol.readBinary()

    if type_id == TType.STRUCT:
        return _read_thrift_struct(protocol)

    if type_id == TType.LIST:
        element_type, size = protocol.readListBegin()
        values = [
            _read_thrift_value(protocol, element_type)
            for _ in range(size)
        ]
        protocol.readListEnd()
        return element_type, values

    if type_id == TType.SET:
        element_type, size = protocol.readSetBegin()
        values = [
            _read_thrift_value(protocol, element_type)
            for _ in range(size)
        ]
        protocol.readSetEnd()
        return element_type, values

    if type_id == TType.MAP:
        key_type, value_type, size = protocol.readMapBegin()
        values = [
            (
                _read_thrift_value(protocol, key_type),
                _read_thrift_value(protocol, value_type),
            )
            for _ in range(size)
        ]
        protocol.readMapEnd()
        return key_type, value_type, values

    raise RuntimeError(f'Unsupported Thrift type id: {type_id}')


def _read_thrift_struct(
    protocol: TCompactProtocol.TCompactProtocol,
) -> ThriftStruct:
    protocol.readStructBegin()
    fields: ThriftStruct = []

    while True:
        _, type_id, field_id = protocol.readFieldBegin()

        if type_id == TType.STOP:
            break

        fields.append(
            (
                field_id,
                type_id,
                _read_thrift_value(protocol, type_id),
            )
        )
        protocol.readFieldEnd()

    protocol.readStructEnd()
    return fields


def _write_thrift_value(
    protocol: TCompactProtocol.TCompactProtocol,
    type_id: int,
    value: object,
) -> None:
    if type_id == TType.BOOL:
        protocol.writeBool(cast(bool, value))
        return

    if type_id == TType.BYTE:
        protocol.writeByte(cast(int, value))
        return

    if type_id == TType.I16:
        protocol.writeI16(cast(int, value))
        return

    if type_id == TType.I32:
        protocol.writeI32(cast(int, value))
        return

    if type_id == TType.I64:
        protocol.writeI64(cast(int, value))
        return

    if type_id == TType.DOUBLE:
        protocol.writeDouble(cast(float, value))
        return

    if type_id == TType.STRING:
        protocol.writeBinary(cast(bytes, value))
        return

    if type_id == TType.STRUCT:
        _write_thrift_struct(protocol, cast(ThriftStruct, value))
        return

    if type_id in {TType.LIST, TType.SET}:
        element_type, raw_values = cast(tuple[int, list[object]], value)

        if type_id == TType.LIST:
            protocol.writeListBegin(element_type, len(raw_values))
        else:
            protocol.writeSetBegin(element_type, len(raw_values))

        for item in raw_values:
            _write_thrift_value(protocol, element_type, item)

        if type_id == TType.LIST:
            protocol.writeListEnd()
        else:
            protocol.writeSetEnd()

        return

    if type_id == TType.MAP:
        key_type, value_type, raw_values = cast(
            tuple[int, int, list[tuple[object, object]]],
            value,
        )
        protocol.writeMapBegin(key_type, value_type, len(raw_values))

        for key, item in raw_values:
            _write_thrift_value(protocol, key_type, key)
            _write_thrift_value(protocol, value_type, item)

        protocol.writeMapEnd()
        return

    raise RuntimeError(f'Unsupported Thrift type id: {type_id}')


def _write_thrift_struct(
    protocol: TCompactProtocol.TCompactProtocol,
    fields: ThriftStruct,
) -> None:
    protocol.writeStructBegin('')

    for field_id, type_id, value in fields:
        protocol.writeFieldBegin('', type_id, field_id)
        _write_thrift_value(protocol, type_id, value)
        protocol.writeFieldEnd()

    protocol.writeFieldStop()
    protocol.writeStructEnd()


def _deserialize_parquet_footer(data: bytes) -> ThriftStruct:
    transport = TTransport.TMemoryBuffer(data)
    protocol = TCompactProtocol.TCompactProtocol(transport)
    return _read_thrift_struct(protocol)


def _serialize_parquet_footer(metadata: ThriftStruct) -> bytes:
    transport = TTransport.TMemoryBuffer()
    protocol = TCompactProtocol.TCompactProtocol(transport)
    _write_thrift_struct(protocol, metadata)
    return transport.getvalue()


def _find_thrift_field(
    fields: ThriftStruct,
    field_id: int,
) -> tuple[int, int, object] | None:
    return next(
        (field for field in fields if field[0] == field_id),
        None,
    )


def _set_thrift_field(
    fields: ThriftStruct,
    field_id: int,
    type_id: int,
    value: object,
) -> None:
    for index, field in enumerate(fields):
        if field[0] == field_id:
            fields[index] = (field_id, type_id, value)
            return

    fields.append((field_id, type_id, value))
    fields.sort(key=lambda field: field[0])


def _patch_parquet_string_schema(
    footer: bytes,
    string_columns: set[str],
) -> bytes:
    metadata = _deserialize_parquet_footer(footer)
    key_value_field = _find_thrift_field(metadata, 5)

    if key_value_field is None or key_value_field[1] != TType.LIST:
        raise RuntimeError('GeoParquet footer has no key-value metadata')

    key_value_type, raw_key_values = cast(
        tuple[int, list[object]],
        key_value_field[2],
    )

    if key_value_type != TType.STRUCT:
        raise RuntimeError('Unexpected Parquet key-value metadata encoding')

    has_geo_metadata = False

    for raw_key_value in raw_key_values:
        key_value = cast(ThriftStruct, raw_key_value)
        key_field = _find_thrift_field(key_value, 1)

        if (
            key_field is not None
            and key_field[1] == TType.STRING
            and key_field[2] == b'geo'
        ):
            has_geo_metadata = True
            break

    if not has_geo_metadata:
        raise RuntimeError(
            'ClickHouse Parquet output is missing GeoParquet metadata'
        )

    schema_field = _find_thrift_field(metadata, 2)

    if schema_field is None or schema_field[1] != TType.LIST:
        raise RuntimeError('Parquet footer has no schema field')

    element_type, raw_schema = cast(
        tuple[int, list[object]],
        schema_field[2],
    )

    if element_type != TType.STRUCT:
        raise RuntimeError('Unexpected Parquet schema encoding')

    if not raw_schema:
        raise RuntimeError('Parquet schema is empty')

    def subtree_end(index: int) -> int:
        element = cast(ThriftStruct, raw_schema[index])
        children_field = _find_thrift_field(element, 5)
        child_count = (
            cast(int, children_field[2])
            if children_field is not None
            else 0
        )
        cursor = index + 1

        for _ in range(child_count):
            cursor = subtree_end(cursor)

        return cursor

    root = cast(ThriftStruct, raw_schema[0])
    root_children_field = _find_thrift_field(root, 5)

    if root_children_field is None or root_children_field[1] != TType.I32:
        raise RuntimeError('Parquet schema root has no child count')

    patched_columns: set[str] = set()
    index = 1

    for _ in range(cast(int, root_children_field[2])):
        element = cast(ThriftStruct, raw_schema[index])
        name_field = _find_thrift_field(element, 4)

        if name_field is not None and name_field[1] == TType.STRING:
            raw_name = cast(bytes, name_field[2])

            try:
                name = raw_name.decode('utf-8')
            except UnicodeDecodeError:
                name = ''

            if name in string_columns:
                physical_type_field = _find_thrift_field(element, 1)

                if (
                    physical_type_field is None
                    or physical_type_field[1] != TType.I32
                    or physical_type_field[2] != _PARQUET_BYTE_ARRAY
                ):
                    raise RuntimeError(
                        f'Expected Parquet BYTE_ARRAY for ClickHouse String '
                        f'column {name!r}'
                    )

                # Parquet STRING is BYTE_ARRAY plus both the modern STRING
                # logical type and the legacy UTF8 converted type for backwards
                # compatibility.
                _set_thrift_field(
                    element,
                    6,
                    TType.I32,
                    _PARQUET_CONVERTED_TYPE_UTF8,
                )
                _set_thrift_field(
                    element,
                    10,
                    TType.STRUCT,
                    [(1, TType.STRUCT, [])],
                )
                patched_columns.add(name)

        index = subtree_end(index)

    missing_columns = string_columns - patched_columns

    # Columns omitted from this particular export are expected. A selected
    # ClickHouse String column, however, must be present in the Parquet schema.
    # The caller therefore passes only selected String columns.
    if missing_columns:
        raise RuntimeError(
            'ClickHouse String columns missing from Parquet schema: '
            + ', '.join(sorted(missing_columns))
        )

    return _serialize_parquet_footer(metadata)


def _patch_parquet_tail(
    tail: bytes,
    string_columns: set[str],
) -> bytes:
    if len(tail) < 8 or tail[-4:] != _PARQUET_MAGIC:
        raise RuntimeError('Invalid Parquet stream trailer')

    footer_size = int.from_bytes(tail[-8:-4], 'little')
    footer_and_trailer_size = footer_size + 8

    if footer_and_trailer_size > len(tail):
        raise RuntimeError(
            'Parquet footer is larger than the retained streaming tail; '
            f'footer={footer_size} bytes, tail={len(tail)} bytes'
        )

    footer_start = len(tail) - footer_and_trailer_size
    prefix = tail[:footer_start]
    footer = tail[footer_start:-8]
    patched_footer = _patch_parquet_string_schema(
        footer=footer,
        string_columns=string_columns,
    )

    return b''.join(
        (
            prefix,
            patched_footer,
            len(patched_footer).to_bytes(4, 'little'),
            _PARQUET_MAGIC,
        )
    )


def stream_clickhouse_geoparquet_query(
    engine: Engine,
    sql: str,
    parameters: dict[str, object],
    string_columns: set[str],
) -> Iterator[bytes]:
    settings: dict[str, object] = {
        'output_format_parquet_geometadata': 1,
        # ClickHouse 26.8 needs binary Parquet BYTE_ARRAY for WKB. This also
        # writes normal String columns as binary; their UTF-8 annotations are
        # restored in the footer below without touching any row data.
        'output_format_parquet_string_as_string': 0,
    }

    with engine.connect() as connection:
        dbapi_connection = cast(
            ClickHouseDbapiConnection,
            connection.connection.driver_connection,
        )
        stream = cast(
            BinaryIO,
            dbapi_connection.client.raw_stream(
                query=sql,
                parameters=parameters,
                settings=settings,
                fmt='Parquet',
            ),
        )

        retained: deque[bytes] = deque()
        retained_size = 0

        try:
            while chunk := stream.read(_STREAM_CHUNK_SIZE):
                retained.append(chunk)
                retained_size += len(chunk)

                # Keep a bounded tail that is comfortably larger than a normal
                # Parquet footer. Yield complete older chunks without copying.
                while (
                    retained
                    and retained_size - len(retained[0]) >= _PARQUET_TAIL_SIZE
                ):
                    old_chunk = retained.popleft()
                    retained_size -= len(old_chunk)
                    yield old_chunk

        finally:
            stream.close()

        tail = b''.join(retained)
        yield _patch_parquet_tail(
            tail=tail,
            string_columns=string_columns,
        )



def query_clickhouse_row_count_limited(
    engine: Engine,
    sql: str,
    parameters: dict[str, object],
) -> int:
    with engine.connect() as connection:
        dbapi_connection = cast(
            ClickHouseDbapiConnection,
            connection.connection.driver_connection,
        )

        result = dbapi_connection.client.query(
            query=sql,
            parameters=parameters,
        )

    return int(result.result_rows[0][0])


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


class JsonResponse(Response):
    media_type = 'application/json'


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


EXPORT_FORMATS: dict[type[Response], tuple[str, str, str]] = {
    CsvResponse: (
        'CSVWithNames',
        'text/csv',
        'csv',
    ),
    TxtResponose: (
        'TabSeparatedWithNames',
        'text/tab-separated-values',
        'tsv',
    ),
    JsonResponse: (
        'JSON',
        'application/json',
        'json',
    ),
    GeoparquetResponse: (
        'Parquet',
        'application/vnd.apache.parquet',
        'parquet',
    ),
}


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
        response_class = route.response_class

        if isinstance(response_class, type) and issubclass(response_class, Response):
            return response_class

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
    '/model/{model_name}/json',
    tags=['Model'],
    summary='Get Model (JSON)',
    response_class=JsonResponse,
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
        table_columns = get_clickhouse_table_columns(
            engine=db_sync,
            table_name=model_name,
            schema_name=schema_name,
        )

        if not table_columns:
            raise HTTPException(
                status_code=404,
                detail='Model/table not found',
            )

        preparer = db_sync.dialect.identifier_preparer

        quoted_schema = preparer.quote_schema(schema_name)
        quoted_table = preparer.quote(model_name)

        response_class = get_response_class(request)
        geometry_encoding: GeometryEncoding = 'native'
        geometry_output_name: Optional[str] = None

        if response_class in {CsvResponse, TxtResponose, XlsxResponse}:
            geometry_encoding = 'wkt'

        selected_columns_sql, selected_geometry_columns, selected_columns = build_select_columns(
            engine=db_sync,
            table_columns=table_columns,
            show_validity=show_validity,
            show_file_source=show_file_source,
            show_geometry=show_geometry,
            geometry_encoding=geometry_encoding,
            geometry_output_name=geometry_output_name,
        )

        if response_class is JsonResponse and len(selected_geometry_columns) > 1:
            raise HTTPException(
                status_code=400,
                detail={
                    'message': (
                        'GeoJSON export supports exactly one selected geometry '
                        'column'
                    ),
                    'columns': selected_geometry_columns,
                },
            )

        if (
            response_class is GeoparquetResponse
            and len(selected_geometry_columns) > 1
        ):
            raise HTTPException(
                status_code=400,
                detail={
                    'message': (
                        'Parquet export currently supports exactly one selected '
                        'geometry column'
                    ),
                    'columns': selected_geometry_columns,
                },
            )

        parquet_is_geoparquet = False

        if response_class is GeoparquetResponse and selected_geometry_columns:
            geometry_column = selected_geometry_columns[0]
            geometry_type = table_columns[geometry_column]
            base_geometry_type = unwrap_clickhouse_type(geometry_type)

            parquet_is_geoparquet = (
                not is_nullable_clickhouse_type(geometry_type)
                and base_geometry_type in CLICKHOUSE_GEOPARQUET_TYPES
            )

            # Non-nullable supported geo types stay native so ClickHouse can
            # produce WKB + GeoParquet metadata in its fast C++ Parquet writer.
            # Nullable/unsupported geo types deliberately fall back to ordinary
            # Parquet with WKT geometry.
            selected_columns_sql, selected_geometry_columns, selected_columns = build_select_columns(
                engine=db_sync,
                table_columns=table_columns,
                show_validity=show_validity,
                show_file_source=show_file_source,
                show_geometry=show_geometry,
                geometry_encoding=(
                    'native' if parquet_is_geoparquet else 'wkt'
                ),
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

        if response_class is XlsxResponse:
            if len(selected_columns) > XLSX_MAX_COLUMNS:
                raise HTTPException(
                    status_code=413,
                    detail={
                        'message': 'XLSX export exceeds the Excel column limit',
                        'max_columns': XLSX_MAX_COLUMNS,
                        'selected_columns': len(selected_columns),
                    },
                )

            # Excel has 1,048,576 worksheet rows. Because pandas writes a
            # header row, at most 1,048,575 data rows can be exported safely.
            # Only probe when the requested LIMIT does not already guarantee
            # that the result fits. The probe stops at max + 1, so this does
            # not run a full COUNT(*) on large exports.
            if limit is None or limit > XLSX_MAX_DATA_ROWS:
                xlsx_probe_parameters = dict(parameters)
                xlsx_probe_parameters['xlsx_probe_limit'] = (
                    XLSX_MAX_DATA_ROWS + 1
                )

                xlsx_probe_sql = f'''
                    SELECT count()
                    FROM (
                        SELECT 1
                        FROM {quoted_schema}.{quoted_table}
                        {where_sql}
                        LIMIT {{xlsx_probe_limit:UInt64}}
                        OFFSET {{offset:UInt64}}
                    )
                '''

                xlsx_row_count = query_clickhouse_row_count_limited(
                    engine=db_sync,
                    sql=xlsx_probe_sql,
                    parameters=xlsx_probe_parameters,
                )

                if xlsx_row_count > XLSX_MAX_DATA_ROWS:
                    raise HTTPException(
                        status_code=413,
                        detail={
                            'message': (
                                'XLSX export exceeds the Excel worksheet row '
                                'limit'
                            ),
                            'max_data_rows': XLSX_MAX_DATA_ROWS,
                            'rows_at_least': xlsx_row_count,
                            'hint': (
                                f'Use limit<={XLSX_MAX_DATA_ROWS} or export '
                                'CSV/Parquet instead.'
                            ),
                        },
                    )

        sql = f'''
            SELECT
                {selected_columns_sql}
            FROM {quoted_schema}.{quoted_table}
            {where_sql}
            {limit_sql}
        '''

        export_format = EXPORT_FORMATS.get(response_class)

        if export_format is not None:
            clickhouse_format, media_type, extension = export_format
            settings: dict[str, object] = {}

            if response_class is CsvResponse:
                settings['format_csv_null_representation'] = ''

            elif response_class is TxtResponose:
                settings['format_tsv_null_representation'] = ''

            if response_class is JsonResponse and selected_geometry_columns:
                clickhouse_format = 'GeoJSON'
                media_type = 'application/geo+json'
                extension = 'geojson'

            elif response_class is GeoparquetResponse and selected_geometry_columns:
                if parquet_is_geoparquet:
                    selected_string_columns = {
                        name
                        for name, type_name in table_columns.items()
                        if (
                            name in selected_columns
                            and is_clickhouse_string_type(type_name)
                        )
                    }

                    return StreamingResponse(
                        stream_clickhouse_geoparquet_query(
                            engine=db_sync,
                            sql=sql,
                            parameters=parameters,
                            string_columns=selected_string_columns,
                        ),
                        media_type='application/vnd.apache.parquet',
                        headers={
                            'Content-Disposition': (
                                'attachment; filename=export.parquet'
                            ),
                        },
                    )

                # Nullable/unsupported geo columns are WKT in an ordinary
                # Parquet file, with no GeoParquet metadata.
                settings['output_format_parquet_geometadata'] = 0

            return StreamingResponse(
                stream_clickhouse_query(
                    engine=db_sync,
                    sql=sql,
                    parameters=parameters,
                    output_format=clickhouse_format,
                    settings=settings,
                ),
                media_type=media_type,
                headers={
                    'Content-Disposition': (
                        f'attachment; filename=export.{extension}'
                    ),
                },
            )

        if response_class is XlsxResponse:
            dataframe = query_clickhouse_dataframe(
                engine=db_sync,
                sql=sql,
                parameters=parameters,
            )

            buffer = io.BytesIO()

            try:
                dataframe.to_excel(
                    buffer,
                    index=False,
                )
            except ValueError as err:
                if 'This sheet is too large!' in str(err):
                    raise HTTPException(
                        status_code=413,
                        detail={
                            'message': (
                                'XLSX export exceeds the Excel worksheet size '
                                'limit'
                            ),
                            'max_data_rows': XLSX_MAX_DATA_ROWS,
                            'max_columns': XLSX_MAX_COLUMNS,
                        },
                    ) from err
                raise

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

