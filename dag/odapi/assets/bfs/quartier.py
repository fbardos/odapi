from dagster import AssetExecutionContext
from dagster import asset
from dagster import define_asset_job

from odapi.resources.duckdb.duckdb import DuckDBResource
from odapi.resources.url.geojson import QuartierBoundaries


@asset(
    compute_kind='python',
    group_name='src_bfs',
    key=['src', 'bfs_quartier'],
)
def bfs_quartier(
    context: AssetExecutionContext,
    geo_quartier: QuartierBoundaries,
    duckdb: DuckDBResource,
):
    gdf = geo_quartier.load_from_zip('ag-b-00.03-95-qg24/json/quart24.geojson')
    engine = duckdb.get_sqlalchemy_engine()
    with engine.connect() as con:
        duckdb_con = con.connection.driver_connection  # need of the duckdb driver...
        assert duckdb_con
        duckdb_con.execute('INSTALL spatial')
        duckdb_con.execute('INSTALL spatial')
        df = gdf.copy()
        df['geometry'] = df.geometry.to_wkb()
        duckdb_con.register('gdf', df)
        duckdb_con.execute('''
            CREATE OR REPLACE TABLE src.bfs_quartier AS
            SELECT
                * EXCLUDE geometry,
                ST_GeomFromWKB(geometry) AS geometry
            FROM gdf
        ''')
        duckdb_con.unregister('gdf')


job = define_asset_job(
    name='job_bfs_quartier',
    selection=[
        'src/bfs_quartier*',
    ],
)
