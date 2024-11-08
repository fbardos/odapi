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
from typing import Optional
import pandas as pd
from typing import Literal
import textwrap
import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import MetaData
from dotenv import load_dotenv
from sqlalchemy import Table
from sqlalchemy import func

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
        * `TODO` Add other geographic levels like cantons and districts

        Later:

        * `TODO` Add possibility to filter by knowledge date other than latest
        * `TODO` Add non-indicator data from different cantons (e.g. Verkehrsdaten)
    """)
)
add_pagination(app)


@app.get(
    '/indicators',
    tags=['Indicators'],
    description='List all available indicators.',
)
async def get_all_available_indicators(db: AsyncEngine = Depends(get_engine)):
    async with db.begin() as conn:
        tbl_seeds = await conn.run_sync(
            lambda conn: Table('seed_indicator', MetaData(bind=None, schema='dbt'), autoload=True, autoload_with=conn)
        )
    stmt = select([tbl_seeds])
    async with db.connect() as conn:
        res = await conn.execute(stmt)
    return res.all()


@app.get(
    '/indicator/csv',
    tags=['Indicators'],
    description='Returns a CSV instaed of JSON. Can be easily parsed by frameworks like pandas or dplyr.',
    response_class=FileResponse('data.csv', media_type='text/csv'),
)
@app.get(
    '/indicator',
    tags=['Indicators'],
    description='Returns a JSON.',
)
async def get_indicator(
        request: Request,
        indicator_id: int = Query(..., description='Select an indicator. If you want to check all possible indicators run path /indicator'),
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
            tbl_api.c.knowledge_code,
            tbl_api.c.knowledge_date,
            tbl_api.c.period_type,
            tbl_api.c.period_code,
            tbl_api.c.period_ref_from,
            tbl_api.c.period_ref,
            tbl_api.c.indicator_value_numeric,
            tbl_api.c.indicator_value_text,
            tbl_api.c.source,
        )
        .where(tbl_api.c.indicator_id == indicator_id)
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
        query = (
            query
            .join(tbl_gemeinde, tbl_api.c.geo_value == tbl_gemeinde.c.gemeinde_bfs_id)
            .join(tbl_bezirk, tbl_gemeinde.c.bezirk_bfs_id == tbl_bezirk.c.bezirk_bfs_id)
            .join(tbl_kanton, tbl_gemeinde.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
        )
        if join_geo:
            query = query.add_columns(
                tbl_gemeinde.c.gemeinde_name.label('geo_name'),
                tbl_bezirk.c.bezirk_bfs_id,
                tbl_bezirk.c.bezirk_name,
                tbl_kanton.c.kanton_bfs_id,
                tbl_kanton.c.kanton_name,
            )
        if join_geo_wkt:
            query = query.add_columns(tbl_gemeinde.c.geometry_wkt.label('geo_wkt'))
    if not disable_pagination:
        query = query.offset(skip).limit(limit)

    query_total =(
        select(func.count())
        .select_from(tbl_api)
        .where(tbl_api.c.indicator_id == indicator_id)
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
        response.headers['Content-Disposition'] = 'attachment; filename=data.csv'
        return response


@app.get(
    '/portrait/csv',
    tags=['Indicators'],
    description='Returns a CSV instaed of JSON. Can be easily parsed by frameworks like pandas or dplyr.',
    response_class=FileResponse('data.csv', media_type='text/csv'),
)
@app.get(
    '/portrait',
    tags=['Indicators'],
    description='Returns a JSON.',
)
async def list_all_indicators_for_one_geometry(
        request: Request,
        geo_code: Literal['polg'] = Query('polg', description='geo_code describes a geographic area. Default is `polg` for `municipality`.'),
        geo_value: int = Query(None, description='ID for a selected `geo_code`. E.g. when `geo_code == polg` is selected, `geo_value == 230` will return Winterthur.'),
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
            tbl_api.c.knowledge_code,
            tbl_api.c.knowledge_date,
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
        query = (
            query
            .join(tbl_gemeinde, tbl_api.c.geo_value == tbl_gemeinde.c.gemeinde_bfs_id)
            .join(tbl_bezirk, tbl_gemeinde.c.bezirk_bfs_id == tbl_bezirk.c.bezirk_bfs_id)
            .join(tbl_kanton, tbl_gemeinde.c.kanton_bfs_id == tbl_kanton.c.kanton_bfs_id)
        )
        if join_geo:
            query = query.add_columns(
                tbl_gemeinde.c.gemeinde_name.label('geo_name'),
                tbl_bezirk.c.bezirk_bfs_id,
                tbl_bezirk.c.bezirk_name,
                tbl_kanton.c.kanton_bfs_id,
                tbl_kanton.c.kanton_name,
            )
        if join_geo_wkt:
            query = query.add_columns(tbl_gemeinde.c.geometry_wkt.label('geo_wkt'))
    if period_ref:
        query = query.where(tbl_api.c.period_ref == period_ref)
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
        response.headers['Content-Disposition'] = 'attachment; filename=data.csv'
        return response

