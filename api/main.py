import io
from fastapi import FastAPI
from fastapi import Depends
from fastapi import Response
from sqlalchemy import MetaData
from fastapi import Request
from fastapi import Query
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy.sql.functions import coalesce
from typing import Optional
import pandas as pd
from typing import Literal
import datetime as dt
import textwrap
import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import MetaData
from dotenv import load_dotenv
from sqlalchemy import Table
from sqlalchemy import func
from enum import Enum

from fastapi.responses import FileResponse
from fastapi_pagination import add_pagination


# DATABASE ###################################################################
load_dotenv()

def get_engine() -> AsyncEngine:
    return create_async_engine(os.environ['SQLALCHEMY_DATABASE_URL'])

def get_metadata():
    return MetaData(bind=None, schema='dbt')


# API ########################################################################
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
        * `DONE` Make API async.
        * `DONE` Add other geographic levels like cantons and districts
        * `DONE` Add possibility to filter by knowledge date other than latest
        * `DONE` Add indicator for [Minergie Houses](https://opendata.swiss/de/dataset/anzahl-minergie-gebaude-in-gemeinden/resource/3ae6d523-748c-466b-8368-04569473338e)
        * `DONE` Add indicator for [Zweitwohnungsanteil](https://opendata.swiss/de/dataset/wohnungsinventar-und-zweitwohnungsanteil)
        * `TODO` Return geojson with json instead of WKT
        * `TODO` Add other indicators from Swisstopo API

        Later:
        * `TODO` Add possibility to get Excel file as response
        * `TODO` Add more indicators only available for districts or cantons
    """)
)
add_pagination(app)


@app.get(
    '/indicators',
    tags=['Indicators'],
    description='List all available indicators.',
)
async def get_all_available_indicators(
    db: AsyncEngine = Depends(get_engine),
    geo_code: Literal['polg', 'bezk', 'kant'] = Query('polg', description='geo_code describes a geographic area. Default is `polg` for `municipality`. Other values are `bezk` for `district` and `kant` for `canton`.'),
):
    async with db.begin() as conn:
        tbl_indicator = await conn.run_sync(
            lambda conn: Table('seed_indicator', MetaData(bind=None, schema='dbt'), autoload=True, autoload_with=conn)
        )
        tbl_api = await conn.run_sync(
            lambda conn: Table('mart_ogd_api', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=conn)
        )
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
    async with db.connect() as conn:
        res = await conn.execute(query)

    return res.all()


@app.get(
    '/indicator/csv',
    tags=['Indicators'],
    description='Returns a CSV instaed of JSON. Can be easily parsed by frameworks like pandas or dplyr.',
    response_class=FileResponse('odapi_data.csv', media_type='text/csv'),
)
@app.get(
    '/indicator',
    tags=['Indicators'],
    description='Returns a JSON.',
)
async def get_indicator(
    request: Request,
    indicator_id: int = Query(..., description='Select an indicator. If you want to check all possible indicators run path /indicator'),
    geo_code: Literal['polg', 'bezk', 'kant'] = Query('polg', description='geo_code describes a geographic area. Default is `polg` for `municipality`. Other values are `bezk` for `district` and `kant` for `canton`.'),
    knowledge_date: str = Query(dt.date.today().strftime('%Y-%m-%d'), description='Allows to query a different state of the data in the past. Format: ISO-8601, Example: `2023-12-31`'),
    skip: int = Query(0, description='Pagination: Skip the first n rows.'),
    limit: int = Query(100, description='Pagination: Limit the size of one page.'),
    disable_pagination: bool = Query(False, description='You can completely disable pagination by setting param `disable_pagination==true`. Please use responsibly.'),
    join_indicator: bool = Query(True, description='Joins information about the indicator.'),
    join_geo: bool = Query(True, description='Joins information about the geometry like its name.'),
    join_geo_wkt: bool = Query(False, description='Joins the geometry itself as WKT. CRS = EPSG:2056 / LV95'),
    db: AsyncEngine = Depends(get_engine)
):
    async with db.begin() as conn:
        tbl_indicator = await conn.run_sync(
            lambda conn: Table('seed_indicator', MetaData(bind=None, schema='dbt'), autoload=True, autoload_with=conn)
        )
        tbl_api = await conn.run_sync(
            lambda conn: Table('mart_ogd_api', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=conn)
        )
        tbl_gemeinde = await conn.run_sync(
            lambda conn: Table('dim_gemeinde_latest', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=conn)
        )
        tbl_bezirk = await conn.run_sync(
            lambda conn: Table('dim_bezirk_latest', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=conn)
        )
        tbl_kanton = await conn.run_sync(
            lambda conn: Table('dim_kanton_latest', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=conn)
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
        .where(tbl_api.c.knowledge_date_from <= dt.date.fromisoformat(knowledge_date))
        .where(coalesce(tbl_api.c.knowledge_date_to, dt.date.fromisoformat('2999-12-31')) > dt.date.fromisoformat(knowledge_date))
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
    if join_geo or join_geo_wkt:
        match geo_code:
            case 'polg':
                query = (
                    query
                    .join(tbl_gemeinde, tbl_api.c.geo_value == tbl_gemeinde.c.gemeinde_bfs_id)
                    .join(tbl_bezirk, tbl_gemeinde.c.bezirk_bfs_id == tbl_bezirk.c.bezirk_bfs_id)
                    .join(tbl_kanton, tbl_gemeinde.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
                )
            case 'bezk':
                query = (
                    query
                    .join(tbl_bezirk, tbl_api.c.geo_value == tbl_bezirk.c.bezirk_bfs_id)
                    .join(tbl_kanton, tbl_bezirk.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
                )
            case 'kant':
                query = (
                    query
                    .join(tbl_kanton, tbl_api.c.geo_value == tbl_kanton.c.kanton_bfs_id)
                )
        if join_geo:
            match geo_code:
                case 'polg':
                    query = query.add_columns(
                        tbl_gemeinde.c.gemeinde_name.label('geo_name'),
                        tbl_bezirk.c.bezirk_bfs_id,
                        tbl_bezirk.c.bezirk_name,
                        tbl_kanton.c.kanton_bfs_id,
                        tbl_kanton.c.kanton_name,
                    )
                case 'bezk':
                    query = query.add_columns(
                        tbl_bezirk.c.bezirk_name.label('geo_name'),
                        tbl_kanton.c.kanton_bfs_id,
                        tbl_kanton.c.kanton_name,
                    )
                case 'kant':
                    query = query.add_columns(
                        tbl_kanton.c.kanton_name.label('geo_name'),
                    )
        if join_geo_wkt:
            match geo_code:
                case 'polg':
                    query = query.add_columns(tbl_gemeinde.c.geometry_wkt.label('geo_wkt'))
                case 'bezk':
                    query = query.add_columns(tbl_bezirk.c.geometry_wkt.label('geo_wkt'))
                case 'kant':
                    query = query.add_columns(tbl_kanton.c.geometry_wkt.label('geo_wkt'))
    if not disable_pagination:
        query = query.offset(skip).limit(limit)

    query_total =(
        select(func.count())
        .select_from(tbl_api)
        .where(tbl_api.c.indicator_id == indicator_id)
        .where(tbl_api.c.geo_code == geo_code)
    )
    async with db.connect() as conn:
        res = await conn.execute(query)
        res_total = await conn.execute(query_total)

    if request.url.path == '/indicator':
        return {
            'total': res_total.scalar_one(),
            'skip': skip,
            'limit': limit,
            'data': res.all(),
        }
    elif request.url.path == '/indicator/csv':
        df = pd.DataFrame.from_dict(
            data=[dict(row) for row in res.all()],
            orient='columns',
        )
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        response = Response(content=buffer.getvalue(), media_type='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.csv'
        return response


@app.get(
    '/portrait/csv',
    tags=['Indicators'],
    description='Returns a CSV instaed of JSON. Can be easily parsed by frameworks like pandas or dplyr.',
    response_class=FileResponse('odapi_data.csv', media_type='text/csv'),
)
@app.get(
    '/portrait',
    tags=['Indicators'],
    description='Returns a JSON.',
)
async def list_all_indicators_for_one_geometry(
    request: Request,
    geo_code: Literal['polg', 'bezk', 'kant'] = Query('polg', description='geo_code describes a geographic area. Default is `polg` for `municipality`. Other values are `bezk` for `district` and `kant` for `canton`.'),
    geo_value: int = Query(..., description='ID for a selected `geo_code`. E.g. when `geo_code == polg` is selected, `geo_value == 230` will return Winterthur.'),
    knowledge_date: str = Query(dt.date.today().strftime('%Y-%m-%d'), description='Allows to query a different state of the data in the past. Format: ISO-8601, Example: `2023-12-31`'),
    join_indicator: bool = Query(True, description='Joins information about the indicator.'),
    join_geo: bool = Query(True, description='Joins information about the geometry like its name.'),
    join_geo_wkt: bool = Query(False, description='Joins the geometry itself as WKT. CRS = EPSG:2056 / LV95'),
    period_ref: Optional[str] = Query(None, description='Allows to filter for a specific period_ref. Format: ISO-8601, Example: `2023-12-31`'),
    db: AsyncEngine = Depends(get_engine)
):
    async with db.begin() as conn:
        tbl_indicator = await conn.run_sync(
            lambda conn: Table('seed_indicator', MetaData(bind=None, schema='dbt'), autoload=True, autoload_with=conn)
        )
        tbl_api = await conn.run_sync(
            lambda conn: Table('mart_ogd_api', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=conn)
        )
        tbl_gemeinde = await conn.run_sync(
            lambda conn: Table('dim_gemeinde_latest', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=conn)
        )
        tbl_bezirk = await conn.run_sync(
            lambda conn: Table('dim_bezirk_latest', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=conn)
        )
        tbl_kanton = await conn.run_sync(
            lambda conn: Table('dim_kanton_latest', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=conn)
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
        .where(tbl_api.c.knowledge_date_from <= dt.date.fromisoformat(knowledge_date))
        .where(coalesce(tbl_api.c.knowledge_date_to, dt.date.fromisoformat('2999-12-31')) > dt.date.fromisoformat(knowledge_date))
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
    if join_geo or join_geo_wkt:
        match geo_code:
            case 'polg':
                query = (
                    query
                    .join(tbl_gemeinde, tbl_api.c.geo_value == tbl_gemeinde.c.gemeinde_bfs_id)
                    .join(tbl_bezirk, tbl_gemeinde.c.bezirk_bfs_id == tbl_bezirk.c.bezirk_bfs_id)
                    .join(tbl_kanton, tbl_gemeinde.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
                )
            case 'bezk':
                query = (
                    query
                    .join(tbl_bezirk, tbl_api.c.geo_value == tbl_bezirk.c.bezirk_bfs_id)
                    .join(tbl_kanton, tbl_bezirk.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
                )
            case 'kant':
                query = (
                    query
                    .join(tbl_kanton, tbl_api.c.geo_value == tbl_kanton.c.kanton_bfs_id)
                )
        if join_geo:
            match geo_code:
                case 'polg':
                    query = query.add_columns(
                        tbl_gemeinde.c.gemeinde_name.label('geo_name'),
                        tbl_bezirk.c.bezirk_bfs_id,
                        tbl_bezirk.c.bezirk_name,
                        tbl_kanton.c.kanton_bfs_id,
                        tbl_kanton.c.kanton_name,
                    )
                case 'bezk':
                    query = query.add_columns(
                        tbl_bezirk.c.bezirk_name.label('geo_name'),
                        tbl_kanton.c.kanton_bfs_id,
                        tbl_kanton.c.kanton_name,
                    )
                case 'kant':
                    query = query.add_columns(
                        tbl_kanton.c.kanton_name.label('geo_name'),
                    )
        if join_geo_wkt:
            match geo_code:
                case 'polg':
                    query = query.add_columns(tbl_gemeinde.c.geometry_wkt.label('geo_wkt'))
                case 'bezk':
                    query = query.add_columns(tbl_bezirk.c.geometry_wkt.label('geo_wkt'))
                case 'kant':
                    query = query.add_columns(tbl_kanton.c.geometry_wkt.label('geo_wkt'))
    if period_ref:
        query = query.where(tbl_api.c.period_ref == dt.date.fromisoformat(period_ref))
    async with db.connect() as conn:
        res = await conn.execute(query)
    if request.url.path == '/portrait':
        return {
            'data': res.all(),
        }
    elif request.url.path == '/portrait/csv':
        df = pd.DataFrame.from_dict(
            data=[dict(row) for row in res.all()],
            orient='columns',
        )
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        response = Response(content=buffer.getvalue(), media_type='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.csv'
        return response


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
async def list_municipalities_by_year(
    request: Request,
    year: int = Query(..., ge=1850, le=dt.datetime.now().year, description='Snapshot year.'),
    join_geo_wkt: bool = Query(False, description='Joins the geometry itself as WKT. CRS = EPSG:2056 / LV95'),
    db: AsyncEngine = Depends(get_engine),
):
    async with db.begin() as conn:
        tbl_gemeinde = await conn.run_sync(
            lambda conn: Table('dim_gemeinde', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=conn)
        )

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
    if join_geo_wkt:
        query = query.add_columns(tbl_gemeinde.c.geometry_wkt.label('geo_wkt'))
    async with db.connect() as conn:
        res = await conn.execute(query)
    if request.url.path == '/municipalities':
        return {
            'data': res.all(),
        }
    elif request.url.path == '/municipalities/csv':
        df = pd.DataFrame.from_dict(
            data=[dict(row) for row in res.all()],
            orient='columns',
        )
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        response = Response(content=buffer.getvalue(), media_type='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.csv'
        return response


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
async def list_districts_by_year(
    request: Request,
    year: int = Query(..., ge=1850, le=dt.datetime.now().year, description='Snapshot year.'),
    join_geo_wkt: bool = Query(False, description='Joins the geometry itself as WKT. CRS = EPSG:2056 / LV95'),
    db: AsyncEngine = Depends(get_engine),
):
    async with db.begin() as conn:
        tbl_bezirk = await conn.run_sync(
            lambda conn: Table('dim_bezirk', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=conn)
        )

    query = (
        select(
            tbl_bezirk.c.snapshot_date,
            tbl_bezirk.c.bezirk_bfs_id,
            tbl_bezirk.c.bezirk_name,
            tbl_bezirk.c.kanton_bfs_id,
        )
        .where(tbl_bezirk.c.snapshot_year == year)
    )
    if join_geo_wkt:
        query = query.add_columns(tbl_bezirk.c.geometry_wkt.label('geo_wkt'))
    async with db.connect() as conn:
        res = await conn.execute(query)
    if request.url.path == '/districts':
        return {
            'data': res.all(),
        }
    elif request.url.path == '/districts/csv':
        df = pd.DataFrame.from_dict(
            data=[dict(row) for row in res.all()],
            orient='columns',
        )
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        response = Response(content=buffer.getvalue(), media_type='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.csv'
        return response


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
async def list_cantons_by_year(
    request: Request,
    year: int = Query(..., ge=1850, le=dt.datetime.now().year, description='Snapshot year.'),
    join_geo_wkt: bool = Query(False, description='Joins the geometry itself as WKT. CRS = EPSG:2056 / LV95'),
    db: AsyncEngine = Depends(get_engine),
):
    async with db.begin() as conn:
        tbl_kanton = await conn.run_sync(
            lambda conn: Table('dim_kanton', MetaData(bind=None, schema='dbt_marts'), autoload=True, autoload_with=conn)
        )

    query = (
        select(
            tbl_kanton.c.snapshot_date,
            tbl_kanton.c.kanton_bfs_id,
            tbl_kanton.c.kanton_name,
            tbl_kanton.c.icc,
        )
        .where(tbl_kanton.c.snapshot_year == year)
    )
    if join_geo_wkt:
        query = query.add_columns(tbl_kanton.c.geometry_wkt.label('geo_wkt'))
    async with db.connect() as conn:
        res = await conn.execute(query)
    if request.url.path == '/cantons':
        return {
            'data': res.all(),
        }
    elif request.url.path == '/cantons/csv':
        df = pd.DataFrame.from_dict(
            data=[dict(row) for row in res.all()],
            orient='columns',
        )
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        response = Response(content=buffer.getvalue(), media_type='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=odapi_data.csv'
        return response

