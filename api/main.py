import datetime as dt
import io
import os
import re
import textwrap
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Optional

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
from sqlalchemy import select
from sqlalchemy.engine import Engine

# DATABASE ###################################################################
load_dotenv()


GROUP_TOTAL_NAME = 'GROUP TOTAL'
GROUP_TOTAL_ID = 1
EXCEL_MAX_ROWS = 1_048_575

DOC__GEOMETRY_MODE = textwrap.dedent("""
    Optional. Will join the coordinates of the geometries. Default is `border_simple_100m`.
    **Be careful, this can create a big response and may take some time**.
    Especially, when no limit is set.

    Possible values:

    | Value | Description | Performance |
    | --- | --- | --- |
    | `point` | Centroid of the border geometry | fast |
    | `border_simple_500_meter` | Simplified border with accuracy of 500 meters | fast |
    | `border_simple_100_meter` | Simplified border with accuracy of 100 meters | medium |
    | `border_simple_50_meter` | Simplified border with accuracy of 50 meters | medium |
    | `border` | Full geometry of the actual border | slow |

""")


DOC__GEO_CODE = textwrap.dedent("""
    Geographic level to return.

    Possible values:

    | Value | EN | DE |
    | --- | --- | --- |
    | `polg` | municipality | Politische Gemeinde |
    | `bezk` | district | Bezirk |
    | `kant` | canton | Kanton |
""")


def get_sync_engine() -> Engine:
    return create_engine(os.environ['SQLALCHEMY_DATABASE_URL_SYNC'])


def get_metadata():
    return MetaData(bind=None, schema='dbt')


@dataclass
class GeoColumnDefinition:
    name: str
    geometry_type: str
    _SRID: int = 4326

    @property
    def column(self) -> Column:
        return Column(self.name, Geometry(geometry_type=self.geometry_type, srid=self._SRID))


class TableDefinition(ABC):
    AUTOLOAD: bool = True
    SCHEMA: str
    TABLE_NAME: str
    COLUMNS: list[GeoColumnDefinition] = []

    @property
    def metadata(cls):
        return MetaData(bind=None, schema=cls.SCHEMA)

    def get_table(self, engine: Engine):

        # Column objects need to be copied, otherwise will throw error,
        # because, the same column object can't be used in multiple tables.
        # Information about the assigned table is stored in the column object.
        coldefs = [
            coldef.column for coldef in self.COLUMNS if isinstance(coldef, GeoColumnDefinition)
        ]
        return Table(
            self.TABLE_NAME,
            self.metadata,
            autoload=self.AUTOLOAD,
            autoload_with=engine,
            *coldefs,
        )


metadata = MetaData()


class TableIndicator(TableDefinition):
    SCHEMA = 'dbt'
    TABLE_NAME = 'seed_indicator'


class TableAvailableIndicator(TableDefinition):
    SCHEMA = 'dbt_marts'
    TABLE_NAME = 'mart_available_indicator'


class TableApi(TableDefinition):
    SCHEMA = 'dbt_marts'
    TABLE_NAME = 'mart_ogd_api'


class TableGemeinde(TableDefinition):
    SCHEMA = 'dbt_marts'
    TABLE_NAME = 'dim_gemeinde'
    COLUMNS = [
        GeoColumnDefinition('geom_border', 'MULTIPOLYGON'),
        GeoColumnDefinition('geom_center', 'POINT'),
    ]


class TableGemeindeLatest(TableDefinition):
    SCHEMA = 'dbt_marts'
    TABLE_NAME = 'dim_gemeinde_latest'
    COLUMNS = [
        GeoColumnDefinition('geom_border', 'MULTIPOLYGON'),
        GeoColumnDefinition('geom_border_simple_100m', 'MULTIPOLYGON'),
        GeoColumnDefinition('geom_center', 'POINT'),
    ]


class TableBezirk(TableDefinition):
    SCHEMA = 'dbt_marts'
    TABLE_NAME = 'dim_bezirk'
    COLUMNS = [
        GeoColumnDefinition('geom_border', 'MULTIPOLYGON'),
        GeoColumnDefinition('geom_center', 'POINT'),
    ]


class TableBezirkLatest(TableDefinition):
    SCHEMA = 'dbt_marts'
    TABLE_NAME = 'dim_bezirk_latest'
    COLUMNS = [
        GeoColumnDefinition('geom_border', 'MULTIPOLYGON'),
        GeoColumnDefinition('geom_center', 'POINT'),
    ]


class TableKanton(TableDefinition):
    SCHEMA = 'dbt_marts'
    TABLE_NAME = 'dim_kanton'
    COLUMNS = [
        GeoColumnDefinition('geom_border', 'MULTIPOLYGON'),
        GeoColumnDefinition('geom_center', 'POINT'),
    ]


class TableKantonLatest(TableDefinition):
    SCHEMA = 'dbt_marts'
    TABLE_NAME = 'dim_kanton_latest'
    COLUMNS = [
        GeoColumnDefinition('geom_border', 'MULTIPOLYGON'),
        GeoColumnDefinition('geom_center', 'POINT'),
    ]


class TableDimSource(Table):
    SCHEMA = 'dbt_marts'
    TABLE_NAME = 'dim_source'
    COLUMNS = [
        Column('id', SMALLINT, primary_key=True),
        Column('source', TEXT),
    ]

    TABLE = Table(
        TABLE_NAME,
        metadata,
        *COLUMNS,
        schema=SCHEMA,
    )


class TableDimGroup(Table):
    SCHEMA = 'dbt_marts'
    TABLE_NAME = 'dim_group'
    COLUMNS = [
        Column('group_id', SMALLINT, primary_key=True),
        Column('group_name', TEXT),
    ]

    TABLE = Table(
        TABLE_NAME,
        metadata,
        *COLUMNS,
        schema=SCHEMA,
    )


class TableDimGroupValue(Table):
    SCHEMA = 'dbt_marts'
    TABLE_NAME = 'dim_group_value'
    COLUMNS = [
        Column('group_value_id', SMALLINT, primary_key=True),
        Column('group_value_name', TEXT),
    ]

    TABLE = Table(
        TABLE_NAME,
        metadata,
        *COLUMNS,
        schema=SCHEMA,
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


class GeoJsonResponse(Response):
    media_type = 'application/geo+json'


class CsvResponse(Response):
    media_type = 'text/csv'

    def __init__(self, content: io.BytesIO, filename: str = 'odapi_data.csv', status_code: int = 200, *args, **kwargs):
        super().__init__(
            content=content.getvalue(),
            status_code=status_code,
            headers={'Content-Disposition': f'attachment; filename={filename}'},
            media_type=self.media_type,
            *args,
            **kwargs
        )


class XlsxResponse(Response):
    media_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

    def __init__(self, content: io.BytesIO, filename: str = 'odapi_data.xlsx', status_code: int = 200, *args, **kwargs):
        super().__init__(
            content=content.getvalue(),
            status_code=status_code,
            headers={'Content-Disposition': f'attachment; filename={filename}'},
            media_type=self.media_type,
            *args,
            **kwargs
        )

class GeoparquetResponse(Response):
    # media_type = 'application/x-parquet'
    media_type = 'application/octet-stream'

    def __init__(self, content: io.BytesIO, filename: str = 'odapi_data.parquet', status_code: int = 200, *args, **kwargs):
        super().__init__(
            content=content.getvalue(),
            status_code=status_code,
            headers={'Content-Disposition': f'attachment; filename={filename}'},
            media_type=self.media_type,
            *args,
            **kwargs
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
            **kwargs
        )


# HELPER FUNC ################################################################
def response_decision(first_path_element: str, request: Request, buffer: io.BytesIO):

    def _transform_geometry_to_wkt(gdf: gpd.GeoDataFrame):
        """Transform geometry column to WKT."""
        if first_path_element == 'values':
            pass
        else:
            gdf['geometry'] = gdf['geometry'].apply(lambda b: wkb.loads(bytes(b)).wkt)
        return gdf

    if re.search(f'^/{first_path_element}.*/parquet$', request.url.path):
        return GeoparquetResponse(content=buffer)
    elif re.search(f'^/{first_path_element}.*/csv$', request.url.path):
        table = pq.read_table(buffer)
        df = table.to_pandas()
        df = _transform_geometry_to_wkt(df)
        buffer_out = io.BytesIO()
        df.to_csv(buffer_out, index=False)
        return CsvResponse(content=buffer_out)
    elif re.search(f'^/{first_path_element}.*/xlsx$', request.url.path):
        table = pq.read_table(buffer)
        df = table.to_pandas()
        df = _transform_geometry_to_wkt(df)
        if len(df.index) > EXCEL_MAX_ROWS:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=(
                    f'Too many rows. Maximum rows for Excel is {EXCEL_MAX_ROWS}. '
                    f'You tried to export {len(gdf.index)} rows. '
                    'Apply more filters or switch to CSV or JSON response'
                ),
            )

        # XlsxWriter seems to be slightly more performant than openpyxl.
        buffer_out = io.BytesIO()
        with pd.ExcelWriter(buffer_out, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='odapi_data', merge_cells=False)
        return XlsxResponse(content=buffer_out)
    elif request.url.path.startswith(f'/{first_path_element}'):
        table = pq.read_table(buffer)
        df = table.to_pandas()
        if first_path_element == 'values':
            return GeoJsonResponse(content=df.to_json())
        else:
            df['geometry'] = df['geometry'].apply(lambda b: wkb.loads(b))
            columns_to_convert = [
                'snapshot_date', 'knowledge_date_from', 'knowledge_date_to', 'period_ref_from', 'period_ref'
            ]
            convert_columns = {col: 'str' for col in df.columns if col in columns_to_convert}
            df = df.astype(convert_columns)

            # After some performance testing, it seems, that the solution with
            # GeoDataFrame.to_geo_dict() is pretty slow. Therefore, build the
            # GeoJSON directly and pass the raw string as custom FastAPI response.
            assert isinstance(df, pd.DataFrame)
            gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
            return GeoJsonResponse(content=gdf.to_json())

def generate_indicator_tree(data: pd.DataFrame) -> Optional[str]:

    ### Prepare the dataframe for parent/child relations
    dfs = []

    # topics
    for i in range(1, 4):
        _df = data[[f'topic_{i}', f'topic_{i+1}']].copy()
        _df.columns = ['parent', 'child']
        _df.dropna(inplace=True)
        dfs.append(_df)

    # indicator
    _df = data.copy()
    _df['filled_topic'] = _df['topic_4'].fillna(_df['topic_3']).fillna(_df['topic_2']).fillna(_df['topic_1'])
    _df['indicator_name'] = '[#' + _df['indicator_id'].astype(str).str.zfill(4) + '] ' + _df['indicator_name'] + ' (' + _df['indicator_unit'] + ')'
    _df = _df[['filled_topic', 'indicator_name']].copy()
    _df.columns = ['parent', 'child']
    _df.dropna(inplace=True)
    dfs.append(_df)

    # concat them together
    df = pd.concat(dfs, ignore_index=True)
    df = df.sort_values(by='parent')

    # Generate network
    G = nx.from_pandas_edgelist(df, source='parent', target='child', create_using=nx.DiGraph)
    text = nx.generate_network_text(G, ascii_only=False, vertical_chains=True)
    buffer = io.StringIO()
    for line in text:
        buffer.write(line+'\n')
    return buffer.getvalue()


# API ########################################################################
app = FastAPI(
    title='ODAPI - Open Data API for Switzerland',
    docs_url='/',
    summary='API for accessing different indicators from different geographic regions in Switzerland.',
    description=textwrap.dedent("""
        ## IMPORTANT

        * This API is under heavy development. Endpoints and responses **can and will change in the future**.
        * This is **not** an official API from the Swiss Government, but just a private iniative from [myself](https://bardos.dev).

        The source code of the API as well as the data transformation can be found [on Github](https://github.com/fbardos/odapi).
    """)
)


# INDICATORS #################################################################
@app.get(
    '/indicators/{geo_code}/txt',
    tags=['Indicators'],
    description='List all available indicators in a human readable format.',
    response_class=TxtResponose,
)
@app.get(
    '/indicators/{geo_code}',
    tags=['Indicators'],
    description='List all available indicators.',
)
def get_all_available_indicators(
    request: Request,
    db_sync: Engine = Depends(get_sync_engine),
    geo_code: GeoCode = Path(
        ...,
        description=textwrap.dedent("""
            Geographic level to return.

            Possible values:

            | Value | EN | DE |
            | --- | --- | --- |
            | `polg` | municipality | Politische Gemeinde |
            | `bezk` | district | Bezirk |
            | `kant` | canton | Kanton |
        """),
    ),
):
    tbl_available_indicator = TableAvailableIndicator().get_table(db_sync)
    query = (
        select(
            tbl_available_indicator.c.indicator_id,
            tbl_available_indicator.c.indicator_name,
            tbl_available_indicator.c.topic_1,
            tbl_available_indicator.c.topic_2,
            tbl_available_indicator.c.topic_3,
            tbl_available_indicator.c.topic_4,
            tbl_available_indicator.c.indicator_unit,
            tbl_available_indicator.c.indicator_description,
        )
        .where(tbl_available_indicator.c.geo_code == geo_code)
    )
    with db_sync.connect() as conn:
        df = pd.read_sql_query(
            sql=query.compile(dialect=db_sync.dialect),
            con=conn,
        )
        assert isinstance(df, pd.DataFrame)
    if re.search(f'^/indicators.*/txt$', request.url.path):
        data = generate_indicator_tree(df)
        return TxtResponose(content=data)
    else:
        return df.to_dict(orient='records')


# INDICATOR ##################################################################
@app.get(
    '/indicator/{geo_code}/{indicator_id}/parquet',
    tags=['Indicator'],
    description=textwrap.dedent("""
        Returns as Parquet file for a selected indicator. This is the fastes way to read data.
        Geometry is stored as serialized WKT in column `geometry`.
    """),
    response_class=GeoparquetResponse,
)
@app.get(
    '/indicator/{geo_code}/{indicator_id}/xlsx',
    tags=['Indicator'],
    description=textwrap.dedent("""
        Returns as Excel file for a selected indicator.

        **Be careful, this can create a big response and may take some time**.
        **Additionally, Microsoft Excel cannot contain a spreadsheet with more than 1,048,576 rows.**
    """),
    response_class=XlsxResponse,
)
@app.get(
    '/indicator/{geo_code}/{indicator_id}/csv',
    tags=['Indicator'],
    description='Returns as CSV for a selected indicator. Can be easily parsed by frameworks like pandas or dplyr.',
    response_class=CsvResponse,
)
@app.get(
    '/indicator/{geo_code}/{indicator_id}',
    tags=['Indicator'],
    description='Returns a GeoJSON for a selected indicator.',
    response_class=GeoJsonResponse,
)
def get_indicator(
    request: Request,
    geo_code: GeoCode = Path(
        ...,
        description=DOC__GEO_CODE,
    ),
    indicator_id: int = Path(..., description='Select an indicator. If you want to check all possible indicators run path `/indicator` first.'),
    geo_value: Optional[int] = Query(None, description='ID for a selected `geo_code`. E.g. when `geo_code == polg` is selected, `geo_value == 230` will return Winterthur.'),
    knowledge_date: Optional[str] = Query(None, examples=[dt.date.today().strftime('%Y-%m-%d')], description='Optional. Allows to query a different state of the data in the past. Format: ISO-8601'),
    period_ref: Optional[str] = Query(None, description='Allows to filter for a specific period_ref. Format: ISO-8601, Example: `2023-12-31`'),
    join_indicator: Optional[bool] = Query(None, description='Optional. Joins information about the indicator.'),
    join_geo: Optional[bool] = Query(None, description='Optional. Joins information about the geometry like its name or its parents.'),
    geometry_mode: Optional[GeometryMode] = Query(
        None,
        description=DOC__GEOMETRY_MODE,
    ),
    skip: Optional[int] = Query(None, examples=[0], description='Optional. Skip the first n rows.'),
    limit: Optional[int] = Query(None, examples=[100], description='Optional. Limit response to the set amount of rows.'),
    expand_all_groups: Optional[bool] = Query(None, description='Optional. Expand all groups in the response.'),
    expand_group_1: Optional[bool] = Query(None, description='Optional. Expand group 1 in the response.'),
    expand_group_2: Optional[bool] = Query(None, description='Optional. Expand group 2 in the response.'),
    expand_group_3: Optional[bool] = Query(None, description='Optional. Expand group 3 in the response.'),
    expand_group_4: Optional[bool] = Query(None, description='Optional. Expand group 4 in the response.'),
    db_sync: Engine = Depends(get_sync_engine),
):
    FIRST_PATH_ELEMENT = 'indicator'
    tbl_indicator = TableIndicator().get_table(db_sync)
    tbl_api = TableApi().get_table(db_sync)
    tbl_gemeinde = TableGemeindeLatest().get_table(db_sync)
    tbl_bezirk =  TableBezirkLatest().get_table(db_sync)
    tbl_kanton = TableKantonLatest().get_table(db_sync)
    tbl_dim_source = TableDimSource().TABLE
    tbl_dim_group_1 = TableDimGroup().TABLE.alias('tbl_dim_group_1')
    tbl_dim_group_2 = TableDimGroup().TABLE.alias('tbl_dim_group_2')
    tbl_dim_group_3 = TableDimGroup().TABLE.alias('tbl_dim_group_3')
    tbl_dim_group_4 = TableDimGroup().TABLE.alias('tbl_dim_group_4')
    tbl_dim_group_value_1 = TableDimGroupValue().TABLE.alias('tbl_dim_group_value_1')
    tbl_dim_group_value_2 = TableDimGroupValue().TABLE.alias('tbl_dim_group_value_2')
    tbl_dim_group_value_3 = TableDimGroupValue().TABLE.alias('tbl_dim_group_value_3')
    tbl_dim_group_value_4 = TableDimGroupValue().TABLE.alias('tbl_dim_group_value_4')
    query = (
        select(
            tbl_api.c.indicator_id,
            tbl_api.c.geo_code,
            tbl_api.c.geo_value,
            tbl_api.c.knowledge_date_from,
            tbl_api.c.knowledge_date_to,
            tbl_api.c.period_type,
            tbl_api.c.period_code,
            tbl_api.c.period_ref_from,
            tbl_api.c.period_ref,
            tbl_dim_group_1.c.group_name.label('group_1_name'),
            tbl_dim_group_value_1.c.group_value_name.label('group_1_value'),
            tbl_dim_group_2.c.group_name.label('group_2_name'),
            tbl_dim_group_value_2.c.group_value_name.label('group_2_value'),
            tbl_dim_group_3.c.group_name.label('group_3_name'),
            tbl_dim_group_value_3.c.group_value_name.label('group_3_value'),
            tbl_dim_group_4.c.group_name.label('group_4_name'),
            tbl_dim_group_value_4.c.group_value_name.label('group_4_value'),
            tbl_api.c.indicator_value_numeric,
            tbl_api.c.indicator_value_text,
            tbl_api.c.source_id,
            tbl_dim_source.c.source,
        )
        .join(tbl_dim_source, tbl_api.c.source_id == tbl_dim_source.c.id)
        .join(tbl_dim_group_1, tbl_api.c.group_1_id == tbl_dim_group_1.c.group_id, isouter=True)
        .join(tbl_dim_group_2, tbl_api.c.group_2_id == tbl_dim_group_2.c.group_id, isouter=True)
        .join(tbl_dim_group_3, tbl_api.c.group_3_id == tbl_dim_group_3.c.group_id, isouter=True)
        .join(tbl_dim_group_4, tbl_api.c.group_4_id == tbl_dim_group_4.c.group_id, isouter=True)
        .join(tbl_dim_group_value_1, tbl_api.c.group_value_1_id == tbl_dim_group_value_1.c.group_value_id, isouter=True)
        .join(tbl_dim_group_value_2, tbl_api.c.group_value_2_id == tbl_dim_group_value_2.c.group_value_id, isouter=True)
        .join(tbl_dim_group_value_3, tbl_api.c.group_value_3_id == tbl_dim_group_value_3.c.group_value_id, isouter=True)
        .join(tbl_dim_group_value_4, tbl_api.c.group_value_4_id == tbl_dim_group_value_4.c.group_value_id, isouter=True)
        .where(tbl_api.c.indicator_id == indicator_id)
        .where(tbl_api.c.geo_code == geo_code)
    )
    if geo_value:
        query = query.where(tbl_api.c.geo_value == geo_value)
    if any([expand_all_groups, expand_group_1]):
        pass  # if any selection is made, do not filter the table
    else:
        query = query.where(tbl_api.c._group_value_1_is_total == True)
    if any([expand_all_groups, expand_group_2]):
        pass  # if any selection is made, do not filter the table
    else:
        query = query.where(tbl_api.c._group_value_2_is_total == True)
    if any([expand_all_groups, expand_group_3]):
        pass  # if any selection is made, do not filter the table
    else:
        query = query.where(tbl_api.c._group_value_3_is_total == True)
    if any([expand_all_groups, expand_group_4]):
        pass  # if any selection is made, do not filter the table
    else:
        query = query.where(tbl_api.c._group_value_4_is_total == True)
    if knowledge_date:
        _knowledge_date = dt.date.fromisoformat(knowledge_date)
        query = (
            query
            .where(tbl_api.c.knowledge_date_from <= _knowledge_date)
            .where(tbl_api.c.knowledge_date_to > _knowledge_date)
        )
    else:
        query = (
            query
            .where(tbl_api.c.knowledge_date_to == None)
        )
    if join_indicator:
        query = (
            query
            .join(tbl_indicator, tbl_api.c.indicator_id == tbl_indicator.c.indicator_id).add_columns(
                tbl_indicator.c.indicator_name,
                tbl_indicator.c.topic_1,
                tbl_indicator.c.topic_2,
                tbl_indicator.c.topic_3,
                tbl_indicator.c.topic_4,
                tbl_indicator.c.indicator_unit,
                tbl_indicator.c.indicator_description,
            )
        )
    # The join of the geometry tables must happen before adding
    # the columns. The geometry tables are used by join_geo AND geometry_mode.
    match geo_code:
        case GeoCode.polg:
            query = (
                query
                .join(tbl_gemeinde, tbl_api.c.geo_value == tbl_gemeinde.c.gemeinde_bfs_id)
                .join(tbl_bezirk, tbl_gemeinde.c.bezirk_bfs_id == tbl_bezirk.c.bezirk_bfs_id)
                .join(tbl_kanton, tbl_gemeinde.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
            )
        case GeoCode.bezk:
            query = (
                query
                .join(tbl_bezirk, tbl_api.c.geo_value == tbl_bezirk.c.bezirk_bfs_id)
                .join(tbl_kanton, tbl_bezirk.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
            )
        case GeoCode.kant:
            query = (
                query
                .join(tbl_kanton, tbl_api.c.geo_value == tbl_kanton.c.kanton_bfs_id)
            )

    # If join_geo is set, add the relevant columns to the query.
    if join_geo:
        match geo_code:
            case GeoCode.polg:
                query = query.add_columns(
                    tbl_gemeinde.c.gemeinde_name.label('geo_name'),
                    tbl_bezirk.c.bezirk_bfs_id,
                    tbl_bezirk.c.bezirk_name,
                    tbl_kanton.c.kanton_bfs_id,
                    tbl_kanton.c.kanton_name,
                )
            case GeoCode.bezk:
                query = query.add_columns(
                    tbl_bezirk.c.bezirk_name.label('geo_name'),
                    tbl_kanton.c.kanton_bfs_id,
                    tbl_kanton.c.kanton_name,
                )
            case GeoCode.kant:
                query = query.add_columns(
                    tbl_kanton.c.kanton_name.label('geo_name'),
                )

    # Column geometry should be the last column in the query.
    if geometry_mode == GeometryMode.point:
        match geo_code:
            case GeoCode.polg:
                query = query.add_columns(tbl_gemeinde.c.geom_center.label('geometry'))
            case GeoCode.bezk:
                query = query.add_columns(tbl_bezirk.c.geom_center.label('geometry'))
            case GeoCode.kant:
                query = query.add_columns(tbl_kanton.c.geom_center.label('geometry'))
    elif geometry_mode == GeometryMode.border:
        match geo_code:
            case GeoCode.polg:
                query = query.add_columns(tbl_gemeinde.c.geom_border.label('geometry'))
            case GeoCode.bezk:
                query = query.add_columns(tbl_bezirk.c.geom_border.label('geometry'))
            case GeoCode.kant:
                query = query.add_columns(tbl_kanton.c.geom_border.label('geometry'))
    elif geometry_mode == GeometryMode.border_simple_50m:
        match geo_code:
            case GeoCode.polg:
                query = query.add_columns(tbl_gemeinde.c.geom_border_simple_50m.label('geometry'))
            case GeoCode.bezk:
                query = query.add_columns(tbl_bezirk.c.geom_border_simple_50m.label('geometry'))
            case GeoCode.kant:
                query = query.add_columns(tbl_kanton.c.geom_border_simple_50m.label('geometry'))
    elif geometry_mode == GeometryMode.border_simple_100m or geometry_mode is None:  # Default
        match geo_code:
            case GeoCode.polg:
                query = query.add_columns(tbl_gemeinde.c.geom_border_simple_100m.label('geometry'))
            case GeoCode.bezk:
                query = query.add_columns(tbl_bezirk.c.geom_border_simple_100m.label('geometry'))
            case GeoCode.kant:
                query = query.add_columns(tbl_kanton.c.geom_border_simple_100m.label('geometry'))
    elif geometry_mode == GeometryMode.border_simple_500m:
        match geo_code:
            case GeoCode.polg:
                query = query.add_columns(tbl_gemeinde.c.geom_border_simple_500m.label('geometry'))
            case GeoCode.bezk:
                query = query.add_columns(tbl_bezirk.c.geom_border_simple_500m.label('geometry'))
            case GeoCode.kant:
                query = query.add_columns(tbl_kanton.c.geom_border_simple_500m.label('geometry'))
    if period_ref:
        query = query.where(tbl_api.c.period_ref == dt.date.fromisoformat(period_ref))
    if skip:
        query = query.offset(skip)
    if limit:
        query = query.limit(limit)

    # Use a new approach with COPY TO STDOUT, which is much (8x) faster than
    # gathering row by row. With the newly installed pg_parquet extension,
    # STDOUT can transfer a prebuilt parquet file, including geometry as WKB.
    with db_sync.connect() as conn:
        psycopg2_connection = conn.connection
        curs = psycopg2_connection.cursor()
        buffer = io.BytesIO()
        copy_sql = f"""
            COPY (
                {query.compile(dialect=db_sync.dialect, compile_kwargs={'literal_binds': True})}
            ) TO STDOUT WITH (FORMAT PARQUET);
        """
        curs.copy_expert(copy_sql, buffer)
        curs.close()
        return response_decision(FIRST_PATH_ELEMENT, request, buffer)


# PORTRAIT ###################################################################
@app.get(
    '/portrait/{geo_code}/{geo_value}/parquet',
    tags=['Portrait'],
    description=textwrap.dedent("""
        Returns as Parquet file for the selected geometry.
        Geometry is stored as serialized WKT in column `geometry`.
    """),
    response_class=GeoparquetResponse,
)
@app.get(
    '/portrait/{geo_code}/{geo_value}/xlsx',
    tags=['Portrait'],
    description=textwrap.dedent("""
        Returns as Excel file for the selected geometry.

        **Be careful, this can create a big response and may take some time**.
        **Additionally, Microsoft Excel cannot contain a spreadsheet with more than 1,048,576 rows.**
    """),
    response_class=XlsxResponse,
)
@app.get(
    '/portrait/{geo_code}/{geo_value}/csv',
    tags=['Portrait'],
    description='Returns a CSV for the selected geometry. Can be easily parsed by frameworks like pandas or dplyr.',
    response_class=CsvResponse,
)
@app.get(
    '/portrait/{geo_code}/{geo_value}',
    tags=['Portrait'],
    description='Returns a GeoJSON for a selected geometry.',
)
def list_all_indicators_for_one_geometry(
    request: Request,
    geo_code: GeoCode = Path(
        ...,
        description=DOC__GEO_CODE,
    ),
    geo_value: int = Path(..., description='ID for a selected `geo_code`. E.g. when `geo_code == polg` is selected, `geo_value == 230` will return Winterthur.'),
    knowledge_date: Optional[str] = Query(None, examples=[dt.date.today().strftime('%Y-%m-%d')], description='Optional. Allows to query a different state of the data in the past. Format: ISO-8601'),
    period_ref: Optional[str] = Query(None, description='Allows to filter for a specific period_ref. Format: ISO-8601, Example: `2023-12-31`'),
    join_indicator: Optional[bool] = Query(None, description='Joins information about the indicator.'),
    join_geo: Optional[bool] = Query(None, description='Joins information about the geometry like its name.'),
    geometry_mode: Optional[GeometryMode] = Query(
        None,
        description=DOC__GEOMETRY_MODE,
    ),
    skip: Optional[int] = Query(None, examples=[0], description='Optional. Skip the first n rows.'),
    limit: Optional[int] = Query(None, examples=[100], description='Optional. Limit response to the set amount of rows.'),
    expand_all_groups: Optional[bool] = Query(None, description='Optional. Expand all groups in the response.'),
    expand_group_1: Optional[bool] = Query(None, description='Optional. Expand group 1 in the response.'),
    expand_group_2: Optional[bool] = Query(None, description='Optional. Expand group 2 in the response.'),
    expand_group_3: Optional[bool] = Query(None, description='Optional. Expand group 3 in the response.'),
    expand_group_4: Optional[bool] = Query(None, description='Optional. Expand group 4 in the response.'),
    db_sync: Engine = Depends(get_sync_engine),
):
    FIRST_PATH_ELEMENT = 'portrait'
    tbl_indicator = TableIndicator().get_table(db_sync)
    tbl_api = TableApi().get_table(db_sync)
    tbl_gemeinde = TableGemeindeLatest().get_table(db_sync)
    tbl_bezirk =  TableBezirkLatest().get_table(db_sync)
    tbl_kanton = TableKantonLatest().get_table(db_sync)
    tbl_dim_source = TableDimSource().TABLE
    tbl_dim_group_1 = TableDimGroup().TABLE.alias('tbl_dim_group_1')
    tbl_dim_group_2 = TableDimGroup().TABLE.alias('tbl_dim_group_2')
    tbl_dim_group_3 = TableDimGroup().TABLE.alias('tbl_dim_group_3')
    tbl_dim_group_4 = TableDimGroup().TABLE.alias('tbl_dim_group_4')
    tbl_dim_group_value_1 = TableDimGroupValue().TABLE.alias('tbl_dim_group_value_1')
    tbl_dim_group_value_2 = TableDimGroupValue().TABLE.alias('tbl_dim_group_value_2')
    tbl_dim_group_value_3 = TableDimGroupValue().TABLE.alias('tbl_dim_group_value_3')
    tbl_dim_group_value_4 = TableDimGroupValue().TABLE.alias('tbl_dim_group_value_4')
    query = (
        select(
            tbl_api.c.indicator_id,
            tbl_api.c.geo_code,
            tbl_api.c.geo_value,
            tbl_api.c.knowledge_date_from,
            tbl_api.c.knowledge_date_to,
            tbl_api.c.period_type,
            tbl_api.c.period_code,
            tbl_api.c.period_ref_from,
            tbl_api.c.period_ref,
            tbl_dim_group_1.c.group_name.label('group_1_name'),
            tbl_dim_group_value_1.c.group_value_name.label('group_1_value'),
            tbl_dim_group_2.c.group_name.label('group_2_name'),
            tbl_dim_group_value_2.c.group_value_name.label('group_2_value'),
            tbl_dim_group_3.c.group_name.label('group_3_name'),
            tbl_dim_group_value_3.c.group_value_name.label('group_3_value'),
            tbl_dim_group_4.c.group_name.label('group_4_name'),
            tbl_dim_group_value_4.c.group_value_name.label('group_4_value'),
            tbl_api.c.indicator_value_numeric,
            tbl_api.c.indicator_value_text,
            tbl_api.c.source_id,
            tbl_dim_source.c.source,
        )
        .join(tbl_dim_source, tbl_api.c.source_id == tbl_dim_source.c.id)
        .join(tbl_dim_group_1, tbl_api.c.group_1_id == tbl_dim_group_1.c.group_id, isouter=True)
        .join(tbl_dim_group_2, tbl_api.c.group_2_id == tbl_dim_group_2.c.group_id, isouter=True)
        .join(tbl_dim_group_3, tbl_api.c.group_3_id == tbl_dim_group_3.c.group_id, isouter=True)
        .join(tbl_dim_group_4, tbl_api.c.group_4_id == tbl_dim_group_4.c.group_id, isouter=True)
        .join(tbl_dim_group_value_1, tbl_api.c.group_value_1_id == tbl_dim_group_value_1.c.group_value_id, isouter=True)
        .join(tbl_dim_group_value_2, tbl_api.c.group_value_2_id == tbl_dim_group_value_2.c.group_value_id, isouter=True)
        .join(tbl_dim_group_value_3, tbl_api.c.group_value_3_id == tbl_dim_group_value_3.c.group_value_id, isouter=True)
        .join(tbl_dim_group_value_4, tbl_api.c.group_value_4_id == tbl_dim_group_value_4.c.group_value_id, isouter=True)
        .where(tbl_api.c.geo_code == geo_code)
        .where(tbl_api.c.geo_value == geo_value)
    )
    if any([expand_all_groups, expand_group_1]):
        pass  # if any selection is made, do not filter the table
    else:
        query = query.where(tbl_api.c._group_value_1_is_total == True)
    if any([expand_all_groups, expand_group_2]):
        pass  # if any selection is made, do not filter the table
    else:
        query = query.where(tbl_api.c._group_value_2_is_total == True)
    if any([expand_all_groups, expand_group_3]):
        pass  # if any selection is made, do not filter the table
    else:
        query = query.where(tbl_api.c._group_value_3_is_total == True)
    if any([expand_all_groups, expand_group_4]):
        pass  # if any selection is made, do not filter the table
    else:
        query = query.where(tbl_api.c._group_value_4_is_total == True)
    if knowledge_date:
        _knowledge_date = dt.date.fromisoformat(knowledge_date)
        query = (
            query
            .where(tbl_api.c.knowledge_date_from <= _knowledge_date)
            .where(tbl_api.c.knowledge_date_to > _knowledge_date)
        )
    else:
        query = (
            query
            .where(tbl_api.c.knowledge_date_to == None)
        )
    if join_indicator:
        query = (
            query
            .join(tbl_indicator, tbl_api.c.indicator_id == tbl_indicator.c.indicator_id).add_columns(
                tbl_indicator.c.indicator_name,
                tbl_indicator.c.topic_1,
                tbl_indicator.c.topic_2,
                tbl_indicator.c.topic_3,
                tbl_indicator.c.topic_4,
                tbl_indicator.c.indicator_unit,
                tbl_indicator.c.indicator_description,
            )
        )

    # The join of the geometry tables must happen before adding
    # the columns. The geometry tables are used by join_geo AND geometry_mode.
    match geo_code:
        case GeoCode.polg:
            query = (
                query
                .join(tbl_gemeinde, tbl_api.c.geo_value == tbl_gemeinde.c.gemeinde_bfs_id)
                .join(tbl_bezirk, tbl_gemeinde.c.bezirk_bfs_id == tbl_bezirk.c.bezirk_bfs_id)
                .join(tbl_kanton, tbl_gemeinde.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
            )
        case GeoCode.bezk:
            query = (
                query
                .join(tbl_bezirk, tbl_api.c.geo_value == tbl_bezirk.c.bezirk_bfs_id)
                .join(tbl_kanton, tbl_bezirk.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
            )
        case GeoCode.kant:
            query = (
                query
                .join(tbl_kanton, tbl_api.c.geo_value == tbl_kanton.c.kanton_bfs_id)
            )

    # If join_geo is set, add the relevant columns to the query.
    if join_geo:
        match geo_code:
            case GeoCode.polg:
                query = query.add_columns(
                    tbl_gemeinde.c.gemeinde_name.label('geo_name'),
                    tbl_bezirk.c.bezirk_bfs_id,
                    tbl_bezirk.c.bezirk_name,
                    tbl_kanton.c.kanton_bfs_id,
                    tbl_kanton.c.kanton_name,
                )
            case GeoCode.bezk:
                query = query.add_columns(
                    tbl_bezirk.c.bezirk_name.label('geo_name'),
                    tbl_kanton.c.kanton_bfs_id,
                    tbl_kanton.c.kanton_name,
                )
            case GeoCode.kant:
                query = query.add_columns(
                    tbl_kanton.c.kanton_name.label('geo_name'),
                )

    # Column geometry should be the last column in the query.
    if geometry_mode == GeometryMode.point:
        match geo_code:
            case GeoCode.polg:
                query = query.add_columns(tbl_gemeinde.c.geom_center.label('geometry'))
            case GeoCode.bezk:
                query = query.add_columns(tbl_bezirk.c.geom_center.label('geometry'))
            case GeoCode.kant:
                query = query.add_columns(tbl_kanton.c.geom_center.label('geometry'))
    elif geometry_mode == GeometryMode.border:
        match geo_code:
            case GeoCode.polg:
                query = query.add_columns(tbl_gemeinde.c.geom_border.label('geometry'))
            case GeoCode.bezk:
                query = query.add_columns(tbl_bezirk.c.geom_border.label('geometry'))
            case GeoCode.kant:
                query = query.add_columns(tbl_kanton.c.geom_border.label('geometry'))
    elif geometry_mode == GeometryMode.border_simple_50m:
        match geo_code:
            case GeoCode.polg:
                query = query.add_columns(tbl_gemeinde.c.geom_border_simple_50m.label('geometry'))
            case GeoCode.bezk:
                query = query.add_columns(tbl_bezirk.c.geom_border_simple_50m.label('geometry'))
            case GeoCode.kant:
                query = query.add_columns(tbl_kanton.c.geom_border_simple_50m.label('geometry'))
    elif geometry_mode == GeometryMode.border_simple_100m or geometry_mode is None:  # Default
        match geo_code:
            case GeoCode.polg:
                query = query.add_columns(tbl_gemeinde.c.geom_border_simple_100m.label('geometry'))
            case GeoCode.bezk:
                query = query.add_columns(tbl_bezirk.c.geom_border_simple_100m.label('geometry'))
            case GeoCode.kant:
                query = query.add_columns(tbl_kanton.c.geom_border_simple_100m.label('geometry'))
    elif geometry_mode == GeometryMode.border_simple_500m:
        match geo_code:
            case GeoCode.polg:
                query = query.add_columns(tbl_gemeinde.c.geom_border_simple_500m.label('geometry'))
            case GeoCode.bezk:
                query = query.add_columns(tbl_bezirk.c.geom_border_simple_500m.label('geometry'))
            case GeoCode.kant:
                query = query.add_columns(tbl_kanton.c.geom_border_simple_500m.label('geometry'))
    if period_ref:
        query = query.where(tbl_api.c.period_ref == dt.date.fromisoformat(period_ref))
    if skip:
        query = query.offset(skip)
    if limit:
        query = query.limit(limit)
    with db_sync.connect() as conn:
        psycopg2_connection = conn.connection
        curs = psycopg2_connection.cursor()
        buffer = io.BytesIO()
        copy_sql = f"""
            COPY (
                {query.compile(dialect=db_sync.dialect, compile_kwargs={'literal_binds': True})}
            ) TO STDOUT WITH (FORMAT PARQUET);
        """
        curs.copy_expert(copy_sql, buffer)
        curs.close()
        return response_decision(FIRST_PATH_ELEMENT, request, buffer)


# NUMBERS ####################################################################
@app.get(
    '/values/{geo_code}/parquet',
    tags=['Values'],
    description=textwrap.dedent("""
        Returns:
        - numbers for all indicators and geo_values for one period_ref
        - as GeoParquet. This is the fastes way to read data.

        This endpoint is designed to return only the numbers of all indicators and geo_values.
        This can then be used e.g. to generate a historgram over all indicators.
        Because the answer can have many rows, it will not join other informations like in other endpoints.

        Geometry is stored as serialized WKT in column `geometry`.
    """),
    response_class=GeoparquetResponse,
)
@app.get(
    '/values/{geo_code}/csv',
    tags=['Values'],
    description=textwrap.dedent("""
        Returns:
        - numbers for all indicators and geo_values for one period_ref
        - as CSV file.

        This endpoint is designed to return only the numbers of all indicators and geo_values.
        This can then be used e.g. to generate a historgram over all indicators.
        Because the answer can have many rows, it will not join other informations like in other endpoints.
    """),
    response_class=CsvResponse,
)
@app.get(
    '/values/{geo_code}',
    tags=['Values'],
    description=textwrap.dedent("""
        Returns:
        - numbers for all indicators and geo_values for one period_ref
        - as JSON response.

        This endpoint is designed to return only the latest numbers of all indicators and geo_values.
        This can then be used e.g. to generate a historgram over all indicators.
        Because the answer can have many rows, it will not join other informations like in other endpoints.
    """),
    response_class=GeoJsonResponse,
)
def get_numbers(
    request: Request,
    geo_code: GeoCode = Path(
        ...,
        description=DOC__GEO_CODE,
    ),
    knowledge_date: Optional[str] = Query(None, examples=[dt.date.today().strftime('%Y-%m-%d')], description='Optional. Allows to query a different state of the data in the past. Format: ISO-8601'),
    skip: Optional[int] = Query(None, examples=[0], description='Optional. Skip the first n rows.'),
    limit: Optional[int] = Query(None, examples=[100], description='Optional. Limit response to the set amount of rows.'),
    db_sync: Engine = Depends(get_sync_engine),
):
    FIRST_PATH_ELEMENT = 'values'
    tbl_api = TableApi().get_table(db_sync)

    query = (
        select(
            tbl_api.c.indicator_id,
            tbl_api.c.geo_value,
            tbl_api.c.indicator_value_numeric,
            tbl_api.c.source_id,
        )
        .distinct(tbl_api.c.indicator_id, tbl_api.c.geo_value)
        .where(tbl_api.c.geo_code == geo_code)
        .where(tbl_api.c._group_value_1_is_total == True)
        .where(tbl_api.c._group_value_2_is_total == True)
        .where(tbl_api.c._group_value_3_is_total == True)
        .where(tbl_api.c._group_value_4_is_total == True)
        .order_by(tbl_api.c.indicator_id, tbl_api.c.geo_value, tbl_api.c.period_ref_from.desc())
    )
    if knowledge_date:
        _knowledge_date = dt.date.fromisoformat(knowledge_date)
        query = (
            query
            .where(tbl_api.c.knowledge_date_from <= _knowledge_date)
            .where(tbl_api.c.knowledge_date_to > _knowledge_date)
        )
    else:
        query = (
            query
            .where(tbl_api.c.knowledge_date_to == None)
        )
    if skip:
        query = query.offset(skip)
    if limit:
        query = query.limit(limit)

    # Use a new approach with COPY TO STDOUT, which is much (8x) faster than
    # gathering row by row. With the newly installed pg_parquet extension,
    # STDOUT can transfer a prebuilt parquet file, including geometry as WKB.
    with db_sync.connect() as conn:
        psycopg2_connection = conn.connection
        curs = psycopg2_connection.cursor()
        buffer = io.BytesIO()
        copy_sql = f"""
            COPY (
                {query.compile(dialect=db_sync.dialect, compile_kwargs={'literal_binds': True})}
            ) TO STDOUT WITH (FORMAT PARQUET);
        """
        curs.copy_expert(copy_sql, buffer)
        curs.close()
        return response_decision(FIRST_PATH_ELEMENT, request, buffer)


# DIM MUNICIPALITIES #########################################################
@app.get(
    '/municipalities/{year}/parquet',
    tags=['DIM Municipalities'],
    description=textwrap.dedent("""
        Returns as Parquet file for a given year (since 1850).
        Geometry is stored as serialized WKT in column `geometry`.
    """),
    response_class=GeoparquetResponse,
)
@app.get(
    '/municipalities/{year}/xlsx',
    tags=['DIM Municipalities'],
    description=textwrap.dedent("""
        Returns municipalities of Switzerland for a given year (since 1850). Returns a XLSX file.

        **Be careful, this can create a big response and may take some time**.
        **Additionally, Microsoft Excel cannot contain a spreadsheet with more than 1,048,576 rows.**

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
    response_class=XlsxResponse,
)
@app.get(
    '/municipalities/{year}/csv',
    tags=['DIM Municipalities'],
    description=textwrap.dedent("""
        Returns municipalities of Switzerland for a given year (since 1850). Returns a CSV.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
    response_class=CsvResponse,
)
@app.get(
    '/municipalities/{year}',
    tags=['DIM Municipalities'],
    description=textwrap.dedent("""
        Returns municipalities of Switzerland for a given year (since 1850). Returns a JSON.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
)
def list_municipalities_by_year(
    request: Request,
    year: int = Path(..., ge=1850, le=dt.datetime.now().year, description='Snapshot year.'),
    geometry_mode: GeometryMode = Query(
        GeometryMode.border_simple_100m,
        description=DOC__GEOMETRY_MODE,
    ),
    skip: Optional[int] = Query(None, examples=[0], description='Optional. Skip the first n rows.'),
    limit: Optional[int] = Query(None, examples=[100], description='Optional. Limit response to the set amount of rows.'),
    db_sync: Engine = Depends(get_sync_engine),
):
    FIRST_PATH_ELEMENT = 'municipalities'
    tbl_gemeinde = TableGemeinde().get_table(db_sync)
    query = (
        select(
            tbl_gemeinde.c.snapshot_date,
            tbl_gemeinde.c.gemeinde_bfs_id,
            tbl_gemeinde.c.gemeinde_hist_bfs_id,
            tbl_gemeinde.c.gemeinde_name,
            tbl_gemeinde.c.bezirk_bfs_id,
            tbl_gemeinde.c.kanton_bfs_id,
        )
        .where(tbl_gemeinde.c.snapshot_year == year)
    )
    match geometry_mode:
        case GeometryMode.point:
            query = query.add_columns(tbl_gemeinde.c.geom_center.label('geometry'))
        case GeometryMode.border:
            query = query.add_columns(tbl_gemeinde.c.geometry)
        case GeometryMode.border_simple_50m:
            query = query.add_columns(tbl_gemeinde.c.geom_border_simple_50m.label('geometry'))
        case GeometryMode.border_simple_100m:
            query = query.add_columns(tbl_gemeinde.c.geom_border_simple_100m.label('geometry'))
        case GeometryMode.border_simple_500m:
            query = query.add_columns(tbl_gemeinde.c.geom_border_simple_500m.label('geometry'))
    if skip:
        query = query.offset(skip)
    if limit:
        query = query.limit(limit)
    with db_sync.connect() as conn:
        psycopg2_connection = conn.connection
        curs = psycopg2_connection.cursor()
        buffer = io.BytesIO()
        copy_sql = f"""
            COPY (
                {query.compile(dialect=db_sync.dialect, compile_kwargs={'literal_binds': True})}
            ) TO STDOUT WITH (FORMAT PARQUET);
        """
        curs.copy_expert(copy_sql, buffer)
        curs.close()
        return response_decision(FIRST_PATH_ELEMENT, request, buffer)


# DIM DISTRICTS ##############################################################
@app.get(
    '/districts/{year}/parquet',
    tags=['DIM Districts'],
    description=textwrap.dedent("""
        Returns as Parquet file for a given year (since 1850).
        Geometry is stored as serialized WKT in column `geometry`.
    """),
    response_class=GeoparquetResponse,
)
@app.get(
    '/districts/{year}/xlsx',
    tags=['DIM Districts'],
    description=textwrap.dedent("""
        Returns districts of Switzerland for a given year (since 1850). Returns a XLSX file.

        **Be careful, this can create a big response and may take some time**.
        **Additionally, Microsoft Excel cannot contain a spreadsheet with more than 1,048,576 rows.**

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
    response_class=XlsxResponse,
)
@app.get(
    '/districts/{year}/csv',
    tags=['DIM Districts'],
    description=textwrap.dedent("""
        Returns districts of Switzerland for a given year (since 1850). Returns a CSV.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
    response_class=CsvResponse,
)
@app.get(
    '/districts/{year}',
    tags=['DIM Districts'],
    description=textwrap.dedent("""
        Returns districts of Switzerland for a given year (since 1850). Returns a JSON.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
)
def list_districts_by_year(
    request: Request,
    year: int = Path(..., ge=1850, le=dt.datetime.now().year, description='Snapshot year.'),
    geometry_mode: GeometryMode = Query(
        GeometryMode.border_simple_50m,
        description=DOC__GEOMETRY_MODE,
    ),
    skip: Optional[int] = Query(None, examples=[0], description='Optional. Skip the first n rows.'),
    limit: Optional[int] = Query(None, examples=[100], description='Optional. Limit response to the set amount of rows.'),
    db_sync: Engine = Depends(get_sync_engine),
):
    FIRST_PATH_ELEMENT = 'districts'
    tbl_bezirk = TableBezirk().get_table(db_sync)

    query = (
        select(
            tbl_bezirk.c.snapshot_date,
            tbl_bezirk.c.bezirk_bfs_id,
            tbl_bezirk.c.bezirk_name,
            tbl_bezirk.c.kanton_bfs_id,
        )
        .where(tbl_bezirk.c.snapshot_year == year)
    )
    match geometry_mode:
        case GeometryMode.point:
            query = query.add_columns(tbl_bezirk.c.geom_center.label('geometry'))
        case GeometryMode.border:
            query = query.add_columns(tbl_bezirk.c.geometry)
        case GeometryMode.border_simple_50m:
            query = query.add_columns(tbl_bezirk.c.geom_border_simple_50m.label('geometry'))
        case GeometryMode.border_simple_100m:
            query = query.add_columns(tbl_bezirk.c.geom_border_simple_100m.label('geometry'))
        case GeometryMode.border_simple_500m:
            query = query.add_columns(tbl_bezirk.c.geom_border_simple_500m.label('geometry'))
    if skip:
        query = query.offset(skip)
    if limit:
        query = query.limit(limit)
    with db_sync.connect() as conn:
        psycopg2_connection = conn.connection
        curs = psycopg2_connection.cursor()
        buffer = io.BytesIO()
        copy_sql = f"""
            COPY (
                {query.compile(dialect=db_sync.dialect, compile_kwargs={'literal_binds': True})}
            ) TO STDOUT WITH (FORMAT PARQUET);
        """
        curs.copy_expert(copy_sql, buffer)
        curs.close()
        return response_decision(FIRST_PATH_ELEMENT, request, buffer)


# DIM PARQUET ################################################################
@app.get(
    '/cantons/{year}/parquet',
    tags=['DIM Cantons'],
    description=textwrap.dedent("""
        Returns as Parquet file for a given year (since 1850).
        Geometry is stored as serialized WKT in column `geometry`.
    """),
    response_class=GeoparquetResponse,
)
@app.get(
    '/cantons/{year}/xlsx',
    tags=['DIM Cantons'],
    description=textwrap.dedent("""
        Returns cantons of Switzerland for a given year (since 1850). Returns a XLSX file.

        **Be careful, this can create a big response and may take some time**.
        **Additionally, Microsoft Excel cannot contain a spreadsheet with more than 1,048,576 rows.**

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
    response_class=XlsxResponse,
)
@app.get(
    '/cantons/{year}/csv',
    tags=['DIM Cantons'],
    description=textwrap.dedent("""
        Returns cantons of Switzerland for a given year (since 1850). Returns a CSV.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
    response_class=CsvResponse,
)
@app.get(
    '/cantons/{year}',
    tags=['DIM Cantons'],
    description=textwrap.dedent("""
        Returns cantons of Switzerland for a given year (since 1850). Returns a JSON.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
)
def list_cantons_by_year(
    request: Request,
    year: int = Path(..., ge=1850, le=dt.datetime.now().year, description='Snapshot year.'),
    geometry_mode: GeometryMode = Query(
        GeometryMode.border_simple_100m,
        description=DOC__GEOMETRY_MODE,
    ),
    skip: Optional[int] = Query(None, examples=[0], description='Optional. Skip the first n rows.'),
    limit: Optional[int] = Query(None, examples=[100], description='Optional. Limit response to the set amount of rows.'),
    db_sync: Engine = Depends(get_sync_engine),
):
    FIRST_PATH_ELEMENT = 'cantons'
    tbl_kanton = TableKanton().get_table(db_sync)

    query = (
        select(
            tbl_kanton.c.snapshot_date,
            tbl_kanton.c.kanton_bfs_id,
            tbl_kanton.c.kanton_name,
            tbl_kanton.c.icc,
        )
        .where(tbl_kanton.c.snapshot_year == year)
    )
    match geometry_mode:
        case GeometryMode.point:
            query = query.add_columns(tbl_kanton.c.geom_center.label('geometry'))
        case GeometryMode.border:
            query = query.add_columns(tbl_kanton.c.geometry)
        case GeometryMode.border_simple_50m:
            query = query.add_columns(tbl_kanton.c.geom_border_simple_50m.label('geometry'))
        case GeometryMode.border_simple_100m:
            query = query.add_columns(tbl_kanton.c.geom_border_simple_100m.label('geometry'))
        case GeometryMode.border_simple_500m:
            query = query.add_columns(tbl_kanton.c.geom_border_simple_500m.label('geometry'))
    if skip:
        query = query.offset(skip)
    if limit:
        query = query.limit(limit)
    with db_sync.connect() as conn:
        psycopg2_connection = conn.connection
        curs = psycopg2_connection.cursor()
        buffer = io.BytesIO()
        copy_sql = f"""
            COPY (
                {query.compile(dialect=db_sync.dialect, compile_kwargs={'literal_binds': True})}
            ) TO STDOUT WITH (FORMAT PARQUET);
        """
        curs.copy_expert(copy_sql, buffer)
        curs.close()
        return response_decision(FIRST_PATH_ELEMENT, request, buffer)


# TESTS ######################################################################
client = TestClient(app)

@pytest.mark.integration
@pytest.mark.parametrize('path', [
    '/indicators/polg',
    '/indicators/bezk',
    '/indicators/kant',
])
def test_valid_response_indicators(path):
    response = client.get(path)
    assert response.status_code == 200
    assert response.headers['content-type'] == 'application/json'


@pytest.mark.integration
@pytest.mark.parametrize('path', [
    '/indicator/polg',
    '/indicator/bezk',
    '/indicator/kant',
])
def test_valid_response_indicator_geo_code(path):
    response = client.get(f'{path}/1?limit=100')
    assert response.status_code == 200
    assert response.headers['content-type'] == 'application/geo+json'


@pytest.mark.integration
@pytest.mark.parametrize('filetype', ('/csv', '/xlsx', ''))
def test_valid_response_indicator_duplicate_request(filetype):
    for _ in range(4):
        response = client.get(f'/indicator/polg/22{filetype}?limit=10')
        assert response.status_code == 200
        match filetype:
            case 'csv/':
                assert response.headers['content-type'] == 'text/csv; charset=utf-8'
            case 'xlsx/':
                assert response.headers['content-type'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            case '':
                assert response.headers['content-type'] == 'application/geo+json'


@pytest.mark.integration
@pytest.mark.parametrize('indicator_id', range(1, 100))
@pytest.mark.parametrize('filetype', ('/csv', '/xlsx', ''))
def test_valid_response_indicator_indicator_id(indicator_id, filetype):
    response = client.get(f'/indicator/polg/{indicator_id}{filetype}?limit=10')
    assert response.status_code == 200
    match filetype:
        case 'csv/':
            assert response.headers['content-type'] == 'text/csv; charset=utf-8'
        case 'xlsx/':
            assert response.headers['content-type'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        case '':
            assert response.headers['content-type'] == 'application/geo+json'


@pytest.mark.integration
@pytest.mark.parametrize('path', [
    '/portrait/polg',
    '/portrait/bezk',
    '/portrait/kant',
])
def test_valid_response_portrait_geo_code(path):
    response = client.get(f'{path}/230?limit=10')
    assert response.status_code == 200
    assert response.headers['content-type'] == 'application/geo+json'


@pytest.mark.integration
@pytest.mark.parametrize('geo_value', (230, 261))
@pytest.mark.parametrize('filetype', ('/csv', '/xlsx', ''))
def test_valid_response_portrait_geo_value(geo_value, filetype):
    response = client.get(f'/portrait/polg/{geo_value}{filetype}?limit=10')
    assert response.status_code == 200
    match filetype:
        case 'csv/':
            assert response.headers['content-type'] == 'text/csv; charset=utf-8'
        case 'xlsx/':
            assert response.headers['content-type'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        case '':
            assert response.headers['content-type'] == 'application/geo+json'


@pytest.mark.integration
@pytest.mark.parametrize('filetype', ('/csv', '/xlsx', ''))
def test_valid_response_portrait_duplicate_request(filetype):
    for _ in range(4):
        response = client.get(f'/portrait/polg/230{filetype}?limit=10')
        assert response.status_code == 200
        match filetype:
            case 'csv/':
                assert response.headers['content-type'] == 'text/csv; charset=utf-8'
            case 'xlsx/':
                assert response.headers['content-type'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            case '':
                assert response.headers['content-type'] == 'application/geo+json'


@pytest.mark.integration
@pytest.mark.parametrize('year', range(1850, dt.datetime.now().year - 1))
@pytest.mark.parametrize('dimension', ('municipalities', 'districts', 'cantons'))
@pytest.mark.parametrize('filetype', ('/csv', '/xlsx', ''))
def test_valid_response_dimensions(year, filetype, dimension):
    response = client.get(f'/{dimension}/{year}{filetype}?limit=10')
    assert response.status_code == 200
    match filetype:
        case 'csv/':
            assert response.headers['content-type'] == 'text/csv; charset=utf-8'
        case 'xlsx/':
            assert response.headers['content-type'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        case '':
            assert response.headers['content-type'] == 'application/geo+json'


@pytest.mark.integration
@pytest.mark.parametrize('year', (1850, 1900, 1945, 1980, 1999, 2003, 2011, 2015, 2016, dt.datetime.now().year - 1))
@pytest.mark.parametrize('dimension', ('municipalities', 'districts', 'cantons'))
@pytest.mark.parametrize('filetype', ('/csv', '/xlsx', ''))
def test_data_rows_dimensions(year, filetype, dimension):
    response = client.get(f'/{dimension}/{year}{filetype}?limit=100')
    match filetype:
        case 'csv/':
            assert response.headers['content-type'] == 'text/csv; charset=utf-8'
            df = pd.read_csv(io.StringIO(response.text))
            if dimension == 'cantons':
                assert len(df) > 20 & len(df) < 30
            else:
                assert len(df) == 100
        case 'xlsx/':
            assert response.headers['content-type'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            df = pd.read_excel(io.BytesIO(response.content))
            if dimension == 'cantons':
                assert len(df) > 20 & len(df) < 30
            else:
                assert len(df) == 100
        case '':
            assert response.headers['content-type'] == 'application/geo+json'
            gdf = gpd.read_file(io.StringIO(response.text))
            if dimension == 'cantons':
                assert len(gdf) > 20 & len(gdf) < 30
            else:
                assert len(gdf) == 100
