import io
from fastapi import FastAPI
from fastapi import Depends
from fastapi import Response
from fastapi import Request
from fastapi import Query
from database import get_db
from database import get_session
from database import table__mart_ogd_api
from database import table__seed_indicators
from database import table__dim_gemeinde
from sqlalchemy.engine import Engine
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy.orm import Session
from typing import Optional
import pandas as pd
from typing import Literal
import textwrap

from fastapi.responses import FileResponse
from fastapi_pagination import add_pagination


app = FastAPI(
    title='ODAPI - Open Data API for Switzerland',
    docs_url='/',
    summary='API for accessing different indicators from different geographic regions in Switzerland.',
    description=textwrap.dedent("""
        ## IMPORTANT

        * This API is under heavy development. Endpoints and responses **can and will change in the future**.
        * This is **not** an official API from the Swiss Government, but just a freetime project from [Fabian Bardos](https://bardos.dev).

        ## Roadmap

        Next:

        * `DONE` Add indicators from BFS STATATLAS
        * `DONE` Provide historized municipal reference
        * `TODO` Add other geographic levels like cantons and districts
        * `TODO` Make API async.

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
def get_all_available_indicators(db: Engine = Depends(get_db)):

    engine, meta = db

    table = Table('seed_indicator', meta, autoload=True, autoload_with=engine)
    stmt = select([table])
    connection = engine.connect()
    res = connection.execute(stmt).fetchall()
    return res


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
def get_indicator(
        request: Request,
        indicator_id: int = Query(..., description='Select an indicator. If you want to check all possible indicators run path /indicator'),
        skip: int = Query(0, description='Pagination: Skip the first n rows.'),
        limit: int = Query(100, description='Pagination: Limit the size of one page.'),
        disable_pagination: bool = Query(False, description='You can completely disable pagination by setting param `disable_pagination==true`. Please use responsibly.'),
        join_indicator: bool = Query(True, description='Joins information about the indicator.'),
        join_geo: bool = Query(True, description='Joins information about the geometry like its name.'),
        join_geo_wkt: bool = Query(False, description='Joins the geometry itself as WKT. CRS = EPSG:2056 / LV95'),
        db: Session = Depends(get_session)
    ):

    table = table__mart_ogd_api
    table_indicator = table__seed_indicators
    table_gemeinde = table__dim_gemeinde

    query = (
        select(
            table.c.indicator_id,
            table.c.geo_code,
            table.c.geo_value,
            table.c.knowledge_code,
            table.c.knowledge_date,
            table.c.period_type,
            table.c.period_code,
            table.c.period_ref_from,
            table.c.period_ref,
            table.c.indicator_value_numeric,
            table.c.indicator_value_text,
            table.c.source,
        )
        .where(table.c.indicator_id == indicator_id)
    )
    if join_indicator:
        query = (
            query
            .join(table_indicator, table.c.indicator_id == table_indicator.c.indicator_id).add_columns(
                table_indicator.c.indicator_name,
                table_indicator.c.topic_1,
                table_indicator.c.topic_2,
                table_indicator.c.topic_3,
                table_indicator.c.topic_4,
                table_indicator.c.indicator_unit,
                table_indicator.c.indicator_description,
            )
        )
    if join_geo or join_geo_wkt:
        query = query.join(table_gemeinde, table.c.geo_value == table_gemeinde.c.gemeinde_bfs_id)
        if join_geo:
            query = query.add_columns(table_gemeinde.c.gemeinde_name.label('geo_name'))
        if join_geo_wkt:
            query = query.add_columns(table_gemeinde.c.geometry_wkt.label('geo_wkt'))
    if not disable_pagination:
        query = query.offset(skip).limit(limit)
    rows = db.execute(query).all()
    total = (
        db
        .query(table)
        .where(table.c.indicator_id == indicator_id)
        .count()
    )
    if request.url.path == '/indicator':
        return {
            'total': total,
            'skip': skip,
            'limit': limit,
            'data': rows,
        }
    elif request.url.path == '/indicator/csv':
        df = pd.DataFrame.from_dict(
            data=[dict(row) for row in rows],
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
def list_all_indicators_for_one_geometry(
        request: Request,
        geo_code: Literal['polg'] = Query('polg', description='geo_code describes a geographic area. Default is `polg` for `municipality`.'),
        geo_value: int = Query(None, description='ID for a selected `geo_code`. E.g. when `geo_code == polg` is selected, `geo_value == 230` will return Winterthur.'),
        join_indicator: bool = Query(True, description='Joins information about the indicator.'),
        join_geo: bool = Query(True, description='Joins information about the geometry like its name.'),
        join_geo_wkt: bool = Query(False, description='Joins the geometry itself as WKT. CRS = EPSG:2056 / LV95'),
        period_ref: Optional[str] = Query(None, description='Allows to filter for a specific period_ref. Format: ISO-8601, Example: `2023-12-31`'),
        db: Session = Depends(get_session)
    ):

    table = table__mart_ogd_api
    table_indicator = table__seed_indicators
    table_gemeinde = table__dim_gemeinde
    query = (
        select(
            table.c.indicator_id,
            table.c.geo_code,
            table.c.geo_value,
            table.c.knowledge_code,
            table.c.knowledge_date,
            table.c.period_type,
            table.c.period_code,
            table.c.period_ref_from,
            table.c.period_ref,
            table.c.indicator_value_numeric,
            table.c.indicator_value_text,
            table.c.source,
        )
        .where(table.c.geo_code == geo_code)
        .where(table.c.geo_value == geo_value)
    )
    if join_indicator:
        query = (
            query
            .join(table_indicator, table.c.indicator_id == table_indicator.c.indicator_id).add_columns(
                table_indicator.c.indicator_name,
                table_indicator.c.topic_1,
                table_indicator.c.topic_2,
                table_indicator.c.topic_3,
                table_indicator.c.topic_4,
                table_indicator.c.indicator_unit,
                table_indicator.c.indicator_description,
            )
        )
    if join_geo or join_geo_wkt:
        query = query.join(table_gemeinde, table.c.geo_value == table_gemeinde.c.gemeinde_bfs_id)
        if join_geo:
            query = query.add_columns(table_gemeinde.c.gemeinde_name.label('geo_name'))
        if join_geo_wkt:
            query = query.add_columns(table_gemeinde.c.geometry_wkt.label('geo_wkt'))
    if period_ref:
        query = query.where(table.c.period_ref == period_ref)
    rows = db.execute(query).all()
    if request.url.path == '/portrait':
        return {
            'data': rows
        }
    elif request.url.path == '/portrait/csv':
        df = pd.DataFrame.from_dict(
            data=[dict(row) for row in rows],
            orient='columns',
        )
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        response = Response(content=buffer.getvalue(), media_type='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=data.csv'
        return response

