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
    summary='API for accessing different indicators from different geographic regions in Switzerland.',
    description=textwrap.dedent("""
        ## IMPORTANT

        * This API is under heavy development. Endpoints and responses **can and will change in the future**.
        * This is **not** an official API from the Swiss Government, but just a freetime project from [Fabian Bardos](https://bardos.dev).

        ## Roadmap

        Next:

        * `DONE` Add indicators from BFS STATATLAS
        * `TODO` Provide historized municipal reference
        * `TODO` Add other geographic levels like cantons and districts
        * `TODO`: Make API async.

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
        indicator_id: int = Query(None, description='Select an indicator. If you want to check all possible indicators run path /indicator'),
        skip: int = Query(0, description='Pagination: Skip the first n rows.'),
        limit: int = Query(1_000, description='Pagination: Limit the size of one page.'),
        disable_pagination: bool = Query(False, description='You can completely disable pagination by setting param `disable_pagination==true`. Please use responsibly.'),
        joins: bool = Query(True, description='Join other tables like municipality or indicator information.'),
        db: Session = Depends(get_session)
    ):

    table = table__mart_ogd_api
    table_indicator = table__seed_indicators
    base_query = (
        db
        .query(table)
        .where(table.c.indicator_id == indicator_id)
    )
    if disable_pagination:
        rows = base_query.all()
    else:
        rows = (
            base_query
            .offset(skip)
            .limit(limit)
            .all()
        )

    indicator = (
        db
        .query(table_indicator)
        .where(table_indicator.c.indicator_id == indicator_id)
        .first()
    )

    total = (
        db
        .query(table)
        .where(table.c.indicator_id == indicator_id)
        .count()
    )

    return_data = [dict(row) for row in rows]

    if joins:

        # Add inidcator information to each row:
        for row in return_data:
            row['indicator'] = dict(indicator)

    # if output_format == 'json':
    if request.url.path == '/indicator':
        return {
            'total': total,
            'skip': skip,
            'limit': limit,
            'data': return_data
        }
    elif request.url.path == '/indicator/csv':
    # elif output_format == 'csv':
        # df = pd.DataFrame(rows)
        df = pd.DataFrame.from_dict(
            data=return_data,
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
        joins: bool = Query(True, description='Join other tables like municipality or indicator information.'),
        period_ref: Optional[str] = Query(..., description='Allows to filter for a specific period_ref. Format: ISO-8601, Example: `2023-12-31`'),
        db: Session = Depends(get_session)
    ):

    table = table__mart_ogd_api
    table_indicator = table__seed_indicators
    if joins:
        query = (
            db
            .query(
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
                table_indicator.c.indicator_name,
                table_indicator.c.topic_1,
                table_indicator.c.topic_2,
                table_indicator.c.topic_3,
                table_indicator.c.topic_4,
                table_indicator.c.indicator_unit,
                table_indicator.c.indicator_description,
            )
            .where(table.c.geo_code == geo_code)
            .where(table.c.geo_value == geo_value)
            .join(table_indicator, table.c.indicator_id == table_indicator.c.indicator_id)
        )
        if period_ref:
            query = query.where(table.c.period_ref == period_ref)
    else:
        query = (
            db
            .query(table)
            .where(table.c.geo_code == geo_code)
            .where(table.c.geo_value == geo_value)
        )
        if period_ref:
            query = query.where(table.c.period_ref == period_ref)

    rows = query.all()

    return_data = [dict(row) for row in rows]
    print(f'RETURN_DATA: {return_data}')
    if request.url.path == '/portrait':
        return {
            'data': return_data
        }
    elif request.url.path == '/portrait/csv':
        df = pd.DataFrame.from_dict(
            data=return_data,
            orient='columns',
        )
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        response = Response(content=buffer.getvalue(), media_type='text/csv')
        response.headers['Content-Disposition'] = 'attachment; filename=data.csv'
        return response

