from dagster import asset
from dagster import ScheduleDefinition
from dagster import define_asset_job
from odapi.resources.postgres.postgres import PostgresResource
from dagster import AssetExecutionContext
from odapi.resources.url.geojson import SwissboundariesTill2015
from odapi.resources.url.gpkg import Swissboundaries
from dagster import TimeWindowPartitionsDefinition


###############################################################################
# UNTIL 2015: Gemeinde, Bezirk, Kanton
###############################################################################
@asset(
    compute_kind='python',
    group_name='src_bfs',
    key=['src', 'bfs_swissboundaries_2015'],
)
def bfs_swissboundaries_2015(
    context: AssetExecutionContext,
    geo_swissboundaries_till_2016: SwissboundariesTill2015,
    db: PostgresResource
):
    gdf = geo_swissboundaries_till_2016.load()
    gdf.to_postgis(
        name='bfs_swissboundaries_2015',
        con=db.get_sqlalchemy_engine(),
        schema='src',
        if_exists='replace',
    )


yearly_partitions_def = TimeWindowPartitionsDefinition(
    cron_schedule="0 0 1 1 *",
    fmt='%Y',
    start='2016',
    end_offset=1,
)


###############################################################################
# FROM 2016: Gemeinde
###############################################################################
@asset(
    compute_kind='python',
    group_name='src_bfs',
    partitions_def=yearly_partitions_def,
    key=['src', 'bfs_swissboundaries_gemeinde'],
)
def bfs_swissboundaries_gemeinde(
    context: AssetExecutionContext,
    geo_swissboundaries: Swissboundaries,
    db: PostgresResource
):
    gdf = geo_swissboundaries.load_gemeinde(year=int(context.partition_key))
    gdf['_snapshot_year'] = int(context.partition_key)
    gdf.to_postgis(
        name='bfs_swissboundaries_gemeinde',
        con=db.get_sqlalchemy_engine(),
        schema='src',
        if_exists='append',
    )

###############################################################################
# FROM 2016: Bezirk
###############################################################################
@asset(
    compute_kind='python',
    group_name='src_bfs',
    partitions_def=yearly_partitions_def,
    key=['src', 'bfs_swissboundaries_bezirk'],
)
def bfs_swissboundaries_bezirk(
    context: AssetExecutionContext,
    geo_swissboundaries: Swissboundaries,
    db: PostgresResource
):
    gdf = geo_swissboundaries.load_bezirk(year=int(context.partition_key))
    gdf['_snapshot_year'] = int(context.partition_key)
    gdf.to_postgis(
        name='bfs_swissboundaries_bezirk',
        con=db.get_sqlalchemy_engine(),
        schema='src',
        if_exists='append',
    )


###############################################################################
# FROM 2016: Kanton
###############################################################################
@asset(
    compute_kind='python',
    group_name='src_bfs',
    partitions_def=yearly_partitions_def,
    key=['src', 'bfs_swissboundaries_kanton'],
)
def bfs_swissboundaries_kanton(
    context: AssetExecutionContext,
    geo_swissboundaries: Swissboundaries,
    db: PostgresResource
):
    gdf = geo_swissboundaries.load_kanton(year=int(context.partition_key))
    gdf['_snapshot_year'] = int(context.partition_key)
    gdf.to_postgis(
        name='bfs_swissboundaries_kanton',
        con=db.get_sqlalchemy_engine(),
        schema='src',
        if_exists='append',
    )


###############################################################################
# FROM 2016: Land
###############################################################################
@asset(
    compute_kind='python',
    group_name='src_bfs',
    partitions_def=yearly_partitions_def,
    key=['src', 'bfs_swissboundaries_land'],
)
def bfs_swissboundaries_land(
    context: AssetExecutionContext,
    geo_swissboundaries: Swissboundaries,
    db: PostgresResource
):
    gdf = geo_swissboundaries.load_land(year=int(context.partition_key))
    gdf['_snapshot_year'] = int(context.partition_key)
    gdf.to_postgis(
        name='bfs_swissboundaries_land',
        con=db.get_sqlalchemy_engine(),
        schema='src',
        if_exists='append',
    )


job_bfs_swissboundaries = define_asset_job(
    name='job_bfs_swissboundaries',
    selection=[
        'src/bfs_swissboundaries_gemeinde*',
        'src/bfs_swissboundaries_bezirk*',
        'src/bfs_swissboundaries_kanton*',
        'src/bfs_swissboundaries_land*',
    ],
    partitions_def=yearly_partitions_def,
)

schedule_bfs_swissboundaries = ScheduleDefinition(
    job=job_bfs_swissboundaries,
    cron_schedule="1 1 1 2 *",
)
