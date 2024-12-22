import io
from fastapi import FastAPI
from fastapi import Depends
from fastapi import Response
from sqlalchemy import MetaData
from fastapi import Request
from fastapi import Query
from sqlalchemy.engine import Engine
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy.sql.functions import coalesce
from geoalchemy2 import Geometry
from typing import Optional
import geopandas as gpd
import datetime as dt
import textwrap
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from dotenv import load_dotenv
from enum import Enum

from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse


# DATABASE ###################################################################
load_dotenv()

def get_sync_engine() -> Engine:
    return create_engine(os.environ['SQLALCHEMY_DATABASE_URL_SYNC'])

def get_metadata():
    return MetaData(bind=None, schema='dbt')


# API ########################################################################
class GeoCode(str, Enum):
    polg = 'polg'
    bezk = 'bezk'
    kant = 'kant'


class GeoJsonResponse(JSONResponse):
    media_type = 'application/geo+json'


app = FastAPI(
    title='ODAPI - Open Data API for Switzerland',
    docs_url='/',
    summary='API for accessing different indicators from different geographic regions in Switzerland.',
    description=textwrap.dedent("""
        ## IMPORTANT

        * This API is under heavy development. Endpoints and responses **can and will change in the future**.
        * This is **not** an official API from the Swiss Government, but just a private iniative from [myself](https://bardos.dev).

        ## Roadmap

        Next:

        * `DONE` Add indicators from BFS STATATLAS
        * `DONE` Provide historized municipal reference
        * `WONT` Make API async.
        * `DONE` Add other geographic levels like cantons and districts
        * `DONE` Add possibility to filter by knowledge date other than latest
        * `DONE` Add indicator for [Minergie Houses](https://opendata.swiss/de/dataset/anzahl-minergie-gebaude-in-gemeinden/resource/3ae6d523-748c-466b-8368-04569473338e)
        * `DONE` Add indicator for [Zweitwohnungsanteil](https://opendata.swiss/de/dataset/wohnungsinventar-und-zweitwohnungsanteil)
        * `DONE` Return geojson with json instead of WKT
        * `DONE` Add possibility to get Excel file as response
        * `TODO` Add other indicators from Swisstopo API

        Later:
        * `TODO` Add more indicators only available for districts or cantons
    """)
)


@app.get(
    '/indicators',
    tags=['Indicators'],
    description='List all available indicators.',
)
def get_all_available_indicators(
    db_sync: Engine = Depends(get_sync_engine),
    geo_code: GeoCode = Query(
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
    tbl_indicator = Table(
        'seed_indicator',
        MetaData(bind=None, schema='dbt'),
        autoload=True,
        autoload_with=db_sync,
    )
    tbl_api = Table('mart_ogd_api', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=db_sync)
    query = (
        select(
            tbl_api.c.indicator_id,
            tbl_indicator.c.indicator_name,
            tbl_indicator.c.topic_1,
            tbl_indicator.c.topic_2,
            tbl_indicator.c.topic_3,
            tbl_indicator.c.topic_4,
            tbl_indicator.c.indicator_unit,
            tbl_indicator.c.indicator_description,
        )
        .distinct()
        .join(tbl_indicator, tbl_api.c.indicator_id == tbl_indicator.c.indicator_id)
        .where(tbl_api.c.geo_code == geo_code)
        .order_by(tbl_api.c.indicator_id.asc())
    )
    with db_sync.connect() as conn:
        df = pd.read_sql_query(
            sql=query.compile(dialect=db_sync.dialect),
            con=conn,
        )
        assert isinstance(df, pd.DataFrame)
    return df.to_dict(orient='records')


@app.get(
    '/indicator/xlsx',
    tags=['Indicators'],
    description='Returns as Excel file for a selected indicator.',
    response_class=FileResponse('odapi_data.xlsx', media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
)
@app.get(
    '/indicator/csv',
    tags=['Indicators'],
    description='Returns as CSV for a selected indicator. Can be easily parsed by frameworks like pandas or dplyr.',
    response_class=FileResponse('odapi_data.csv', media_type='text/csv'),
)
@app.get(
    '/indicator',
    tags=['Indicators'],
    description='Returns a GeoJSON for a selected indicator.',
    response_class=GeoJsonResponse,
)
def get_indicator(
    request: Request,
    indicator_id: int = Query(..., description='Select an indicator. If you want to check all possible indicators run path `/indicator` first.'),
    geo_code: GeoCode = Query(
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
    knowledge_date: Optional[str] = Query(None, example=dt.date.today().strftime('%Y-%m-%d'), description='Optional. Allows to query a different state of the data in the past. Format: ISO-8601'),
    period_ref: Optional[str] = Query(None, description='Allows to filter for a specific period_ref. Format: ISO-8601, Example: `2023-12-31`'),
    join_indicator: bool = Query(True, description='Optional. Joins information about the indicator.'),
    join_geo: bool = Query(True, description='Optional. Joins information about the geometry like its name or its parents.'),
    skip: Optional[int] = Query(0, description='Optional. Skip the first n rows.'),
    limit: Optional[int] = Query(10, description='Optional. Limit response to the set amount of rows.'),
    db_sync: Engine = Depends(get_sync_engine),
):
    tbl_indicator = Table(
        'seed_indicator',
        MetaData(bind=None, schema='dbt'),
        autoload=True,
        autoload_with=db_sync,
    )
    tbl_api = Table('mart_ogd_api', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=db_sync)
    tbl_gemeinde = Table(
        'dim_gemeinde_latest',
        MetaData(bind=None, schema='dbt_marts'),
        Column('geometry', Geometry(geometry_type='MULTIPOLYGON', srid=2056)),
        autoload=True,
        autoload_with=db_sync,
    )
    tbl_bezirk =  Table(
        'dim_bezirk_latest',
        MetaData(bind=None, schema='dbt_marts'),
        Column('geometry', Geometry(geometry_type='MULTIPOLYGON', srid=2056)),
        autoload=True,
        autoload_with=db_sync,
    )
    tbl_kanton = Table(
        'dim_kanton_latest',
        MetaData(bind=None, schema='dbt_marts'),
        Column('geometry', Geometry(geometry_type='MULTIPOLYGON', srid=2056)),
        autoload=True,
        autoload_with=db_sync,
    )
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
            tbl_api.c.indicator_value_numeric,
            tbl_api.c.indicator_value_text,
            tbl_api.c.source,
        )
        .where(tbl_api.c.indicator_id == indicator_id)
        .where(tbl_api.c.geo_code == geo_code)
    )
    if knowledge_date:
        _knowledge_date = dt.date.fromisoformat(knowledge_date)
        query = (
            query
            .where(tbl_api.c.knowledge_date_from <= _knowledge_date)
            .where(coalesce(tbl_api.c.knowledge_date_to, dt.date.fromisoformat('2999-12-31')) > _knowledge_date)
        )
    else:
        _knowledge_date = dt.date.today() + dt.timedelta(days=1)
        query = (
            query
            .where(tbl_api.c.knowledge_date_from <= _knowledge_date)
            .where(coalesce(tbl_api.c.knowledge_date_to, dt.date.fromisoformat('2999-12-31')) > _knowledge_date)
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
    match geo_code:
        case GeoCode.polg:
            query = (
                query
                .join(tbl_gemeinde, tbl_api.c.geo_value == tbl_gemeinde.c.gemeinde_bfs_id)
                .join(tbl_bezirk, tbl_gemeinde.c.bezirk_bfs_id == tbl_bezirk.c.bezirk_bfs_id)
                .join(tbl_kanton, tbl_gemeinde.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
                .add_columns(tbl_gemeinde.c.geometry)
            )
        case GeoCode.bezk:
            query = (
                query
                .join(tbl_bezirk, tbl_api.c.geo_value == tbl_bezirk.c.bezirk_bfs_id)
                .join(tbl_kanton, tbl_bezirk.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
                .add_columns(tbl_bezirk.c.geometry)
            )
        case GeoCode.kant:
            query = (
                query
                .join(tbl_kanton, tbl_api.c.geo_value == tbl_kanton.c.kanton_bfs_id)
                .add_columns(tbl_kanton.c.geometry)
            )
    if period_ref:
        query = query.where(tbl_api.c.period_ref == dt.date.fromisoformat(period_ref))
    if skip:
        query = query.offset(skip)
    if limit:
        query = query.limit(limit)
    with db_sync.connect() as conn:
        gdf = gpd.read_postgis(
            sql=query.compile(dialect=db_sync.dialect),
            con=conn,
            geom_col='geometry',
            crs='EPSG:2056',
        )
        assert isinstance(gdf, gpd.GeoDataFrame)

    # Form response...
    if request.url.path == '/indicator':
        return gdf.to_geo_dict()
    elif request.url.path == '/indicator/csv':
        buffer = io.StringIO()
        gdf.to_csv(buffer, index=False)
        response = Response(content=buffer.getvalue(), media_type='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.csv'
        return response
    elif request.url.path == '/indicator/xlsx':
        buffer = io.BytesIO()
        gdf.to_excel(buffer, index=False, sheet_name='data')
        response = Response(content=buffer.getvalue(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.xlsx'
        return response


@app.get(
    '/portrait/xlsx',
    tags=['Indicators'],
    description='Returns as Excel file for the selected geometry.',
    response_class=FileResponse('odapi_data.xlsx', media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
)
@app.get(
    '/portrait/csv',
    tags=['Indicators'],
    description='Returns a CSV for the selected geometry. Can be easily parsed by frameworks like pandas or dplyr.',
    response_class=FileResponse('odapi_data.csv', media_type='text/csv'),
)
@app.get(
    '/portrait',
    tags=['Indicators'],
    description='Returns a GeoJSON for a selected geometry.',
)
def list_all_indicators_for_one_geometry(
    request: Request,
    geo_code: GeoCode = Query(
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
    geo_value: int = Query(..., description='ID for a selected `geo_code`. E.g. when `geo_code == polg` is selected, `geo_value == 230` will return Winterthur.'),
    knowledge_date: Optional[str] = Query(None, example=dt.date.today().strftime('%Y-%m-%d'), description='Optional. Allows to query a different state of the data in the past. Format: ISO-8601'),
    period_ref: Optional[str] = Query(None, description='Allows to filter for a specific period_ref. Format: ISO-8601, Example: `2023-12-31`'),
    join_indicator: bool = Query(True, description='Joins information about the indicator.'),
    join_geo: bool = Query(True, description='Joins information about the geometry like its name.'),
    skip: Optional[int] = Query(0, description='Optional. Skip the first n rows.'),
    limit: Optional[int] = Query(10, description='Optional. Limit response to the set amount of rows.'),
    db_sync: Engine = Depends(get_sync_engine),
):
    tbl_indicator = Table(
        'seed_indicator',
        MetaData(bind=None, schema='dbt'),
        autoload=True,
        autoload_with=db_sync,
    )
    tbl_api = Table('mart_ogd_api', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=db_sync)
    tbl_gemeinde = Table(
        'dim_gemeinde_latest',
        MetaData(bind=None, schema='dbt_marts'),
        Column('geometry', Geometry(geometry_type='MULTIPOLYGON', srid=2056)),
        autoload=True,
        autoload_with=db_sync,
    )
    tbl_bezirk =  Table(
        'dim_bezirk_latest',
        MetaData(bind=None, schema='dbt_marts'),
        Column('geometry', Geometry(geometry_type='MULTIPOLYGON', srid=2056)),
        autoload=True,
        autoload_with=db_sync,
    )
    tbl_kanton = Table(
        'dim_kanton_latest',
        MetaData(bind=None, schema='dbt_marts'),
        Column('geometry', Geometry(geometry_type='MULTIPOLYGON', srid=2056)),
        autoload=True,
        autoload_with=db_sync,
    )
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
            tbl_api.c.indicator_value_numeric,
            tbl_api.c.indicator_value_text,
            tbl_api.c.source,
        )
        .where(tbl_api.c.geo_code == geo_code)
        .where(tbl_api.c.geo_value == geo_value)
    )
    if knowledge_date:
        _knowledge_date = dt.date.fromisoformat(knowledge_date)
        query = (
            query
            .where(tbl_api.c.knowledge_date_from <= _knowledge_date)
            .where(coalesce(tbl_api.c.knowledge_date_to, dt.date.fromisoformat('2999-12-31')) > _knowledge_date)
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
    match geo_code:
        case GeoCode.polg:
            query = (
                query
                .join(tbl_gemeinde, tbl_api.c.geo_value == tbl_gemeinde.c.gemeinde_bfs_id)
                .join(tbl_bezirk, tbl_gemeinde.c.bezirk_bfs_id == tbl_bezirk.c.bezirk_bfs_id)
                .join(tbl_kanton, tbl_gemeinde.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
                .add_columns(tbl_gemeinde.c.geometry)
            )
        case GeoCode.bezk:
            query = (
                query
                .join(tbl_bezirk, tbl_api.c.geo_value == tbl_bezirk.c.bezirk_bfs_id)
                .join(tbl_kanton, tbl_bezirk.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
                .add_columns(tbl_bezirk.c.geometry)
            )
        case GeoCode.kant:
            query = (
                query
                .join(tbl_kanton, tbl_api.c.geo_value == tbl_kanton.c.kanton_bfs_id)
                .add_columns(tbl_kanton.c.geometry)
            )
    if period_ref:
        query = query.where(tbl_api.c.period_ref == dt.date.fromisoformat(period_ref))
    if skip:
        query = query.offset(skip)
    if limit:
        query = query.limit(limit)
    with db_sync.connect() as conn:
        gdf = gpd.read_postgis(
            sql=query.compile(dialect=db_sync.dialect),
            con=conn,
            geom_col='geometry',
            crs='EPSG:2056',
        )
        assert isinstance(gdf, gpd.GeoDataFrame)
    
    # Form response...
    if request.url.path == '/portrait':
        return gdf.to_geo_dict()
    elif request.url.path == '/portrait/csv':
        buffer = io.StringIO()
        gdf.to_csv(buffer, index=False)
        response = Response(content=buffer.getvalue(), media_type='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.csv'
        return response
    elif request.url.path == '/portrait/xlsx':
        buffer = io.BytesIO()
        gdf.to_excel(buffer, index=False, sheet_name='data')
        response = Response(content=buffer.getvalue(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.xlsx'
        return response


@app.get(
    '/municipalities/xlsx',
    tags=['Dimensions'],
    description=textwrap.dedent("""
        Returns municipalities of Switzerland for a given year (since 1850). Returns a XLSX file.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
    response_class=FileResponse('odapi_data.xlsx', media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
)
@app.get(
    '/municipalities/csv',
    tags=['Dimensions'],
    description=textwrap.dedent("""
        Returns municipalities of Switzerland for a given year (since 1850). Returns a CSV.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
    response_class=FileResponse('odapi_data.csv', media_type='text/csv'),
)
@app.get(
    '/municipalities',
    tags=['Dimensions'],
    description=textwrap.dedent("""
        Returns municipalities of Switzerland for a given year (since 1850). Returns a JSON.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
)
def list_municipalities_by_year(
    request: Request,
    year: int = Query(..., ge=1850, le=dt.datetime.now().year, description='Snapshot year.'),
    db_sync: Engine = Depends(get_sync_engine),
):
    tbl_gemeinde = Table(
        'dim_gemeinde',
        MetaData(bind=None, schema='dbt_marts'),
        Column('geometry', Geometry(geometry_type='MULTIPOLYGON', srid=2056)),
        autoload=True,
        autoload_with=db_sync,
    )
    query = (
        select(
            tbl_gemeinde.c.snapshot_date,
            tbl_gemeinde.c.gemeinde_bfs_id,
            tbl_gemeinde.c.gemeinde_hist_bfs_id,
            tbl_gemeinde.c.gemeinde_name,
            tbl_gemeinde.c.bezirk_bfs_id,
            tbl_gemeinde.c.kanton_bfs_id,
            tbl_gemeinde.c.geometry,
        )
        .where(tbl_gemeinde.c.snapshot_year == year)
    )
    with db_sync.connect() as conn:
        gdf = gpd.read_postgis(
            sql=query.compile(dialect=db_sync.dialect),
            con=conn,
            geom_col='geometry',
            crs='EPSG:2056',
        )
        assert isinstance(gdf, gpd.GeoDataFrame)
    if request.url.path == '/municipalities':
        return gdf.to_geo_dict()
    elif request.url.path == '/municipalities/csv':
        buffer = io.StringIO()
        gdf.to_csv(buffer, index=False)
        response = Response(content=buffer.getvalue(), media_type='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.csv'
        return response
    elif request.url.path == '/municipalities/xlsx':
        buffer = io.BytesIO()
        gdf.to_excel(buffer, index=False, sheet_name='data')
        response = Response(content=buffer.getvalue(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.xlsx'
        return response


@app.get(
    '/districts/xlsx',
    tags=['Dimensions'],
    description=textwrap.dedent("""
        Returns districts of Switzerland for a given year (since 1850). Returns a XLSX file.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
    response_class=FileResponse('odapi_data.xlsx', media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
)
@app.get(
    '/districts/csv',
    tags=['Dimensions'],
    description=textwrap.dedent("""
        Returns districts of Switzerland for a given year (since 1850). Returns a CSV.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
    response_class=FileResponse('odapi_data.csv', media_type='text/csv'),
)
@app.get(
    '/districts',
    tags=['Dimensions'],
    description=textwrap.dedent("""
        Returns districts of Switzerland for a given year (since 1850). Returns a JSON.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
)
def list_districts_by_year(
    request: Request,
    year: int = Query(..., ge=1850, le=dt.datetime.now().year, description='Snapshot year.'),
    db_sync: Engine = Depends(get_sync_engine),
):
    tbl_bezirk = Table('dim_bezirk', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=db_sync)

    query = (
        select(
            tbl_bezirk.c.snapshot_date,
            tbl_bezirk.c.bezirk_bfs_id,
            tbl_bezirk.c.bezirk_name,
            tbl_bezirk.c.kanton_bfs_id,
            tbl_bezirk.c.geometry,
        )
        .where(tbl_bezirk.c.snapshot_year == year)
    )
    with db_sync.connect() as conn:
        gdf = gpd.read_postgis(
            sql=query.compile(dialect=db_sync.dialect),
            con=conn,
            geom_col='geometry',
            crs='EPSG:2056',
        )
        assert isinstance(gdf, gpd.GeoDataFrame)
    if request.url.path == '/districts':
        return gdf.to_geo_dict()
    elif request.url.path == '/districts/csv':
        buffer = io.StringIO()
        gdf.to_csv(buffer, index=False)
        response = Response(content=buffer.getvalue(), media_type='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.csv'
        return response
    elif request.url.path == '/districts/xlsx':
        buffer = io.BytesIO()
        gdf.to_excel(buffer, index=False, sheet_name='data')
        response = Response(content=buffer.getvalue(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.xlsx'
        return response


@app.get(
    '/cantons/xlsx',
    tags=['Dimensions'],
    description=textwrap.dedent("""
        Returns cantons of Switzerland for a given year (since 1850). Returns a XLSX file.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
    response_class=FileResponse('odapi_data.xlsx', media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
)
@app.get(
    '/cantons/csv',
    tags=['Dimensions'],
    description=textwrap.dedent("""
        Returns cantons of Switzerland for a given year (since 1850). Returns a CSV.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
    response_class=FileResponse('odapi_data.csv', media_type='text/csv'),
)
@app.get(
    '/cantons',
    tags=['Dimensions'],
    description=textwrap.dedent("""
        Returns cantons of Switzerland for a given year (since 1850). Returns a JSON.

        Sources:
        * [Until 2015: data.geo.admin.ch](https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json)
        * [From 2016: Swissboundaries3D](https://www.swisstopo.admin.ch/de/landschaftsmodell-swissboundaries3d)
    """),
)
def list_cantons_by_year(
    request: Request,
    year: int = Query(..., ge=1850, le=dt.datetime.now().year, description='Snapshot year.'),
    db_sync: Engine = Depends(get_sync_engine),
):
    tbl_kanton = Table('dim_kanton', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=db_sync)

    query = (
        select(
            tbl_kanton.c.snapshot_date,
            tbl_kanton.c.kanton_bfs_id,
            tbl_kanton.c.kanton_name,
            tbl_kanton.c.icc,
            tbl_kanton.c.geometry,
        )
        .where(tbl_kanton.c.snapshot_year == year)
    )
    with db_sync.connect() as conn:
        gdf = gpd.read_postgis(
            sql=query.compile(dialect=db_sync.dialect),
            con=conn,
            geom_col='geometry',
            crs='EPSG:2056',
        )
        assert isinstance(gdf, gpd.GeoDataFrame)
    if request.url.path == '/cantons':
        return gdf.to_geo_dict()
    elif request.url.path == '/cantons/csv':
        buffer = io.StringIO()
        gdf.to_csv(buffer, index=False)
        response = Response(content=buffer.getvalue(), media_type='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.csv'
        return response
    elif request.url.path == '/cantons/xlsx':
        buffer = io.BytesIO()
        gdf.to_excel(buffer, index=False, sheet_name='data')
        response = Response(content=buffer.getvalue(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.xlsx'
        return response
