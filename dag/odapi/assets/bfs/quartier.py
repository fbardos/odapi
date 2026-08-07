from dagster import AssetExecutionContext
from dagster import asset
from dagster import define_asset_job

from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.url.geojson import QuartierBoundaries


@asset(
    compute_kind='python',
    group_name='src_bfs',
    key=['src', 'bfs_quartier'],
)
def bfs_quartier(
    context: AssetExecutionContext,
    geo_quartier: QuartierBoundaries,
    db: PostgresResource,
):
    gdf = geo_quartier.load_from_zip('ag-b-00.03-95-qg24/json/quart24.geojson')
    gdf.to_postgis(
        name='bfs_quartier',
        con=db.get_sqlalchemy_engine(),
        schema='src',
        if_exists='replace',
    )


job = define_asset_job(
    name='job_bfs_quartier',
    selection=[
        'src/bfs_quartier*',
    ],
)
