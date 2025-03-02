import datetime as dt
from abc import ABC
from dataclasses import dataclass
from typing import Optional

from dagster import AssetsDefinition
from dagster import HookContext
from dagster import MetadataValue
from dagster import ScheduleDefinition
from dagster import asset
from dagster import define_asset_job
from dagster import success_hook
from pytz import timezone

from odapi.resources.extract.extract_handler import ExtractHandler
from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.url.geoadmin import GeoAdminResource


@dataclass(frozen=True)
class GeoAdminConfiguration(ABC):
    """
    Expects .gpkg file URLs.

    Find new datasources here:
        https://data.geo.admin.ch/, or
        https://data.geo.admin.ch/browser


    Example (Zweitwohnungen):

        collection: ch.are.wohnungsinventar-zweitwohnungsanteil

    """

    name: str
    collection: str
    feature_id_regex: Optional[str] = None
    artefact_type: Optional[str] = None
    asset_key_regex: Optional[str] = None
    layer: Optional[str] = None


SOURCES = [
    GeoAdminConfiguration(
        name='zweitwohnungen',
        collection='ch.are.wohnungsinventar-zweitwohnungsanteil',
        # artefact_type = 'application/x.shapefile+zip',
        # asset_key_regex = r'wohnungsinventar-zweitwohnungsanteil_\d{4}-\d{2}_2056\.shp\.zip',
        artefact_type='application/geopackage+sqlite3',
        asset_key_regex=r'wohnungsinventar-zweitwohnungsanteil_\d{4}(-\d{2})?_2056\.gpkg',
    ),
]


def swisstopo_api_factory(geo_config: GeoAdminConfiguration) -> AssetsDefinition:

    @asset(
        compute_kind='python',
        group_name='src_geoadmin',
        key=['src', f'swisstopo_{geo_config.name}'],
    )
    def _asset_swisstopo(
        context,
        geoadmin: GeoAdminResource,
        extractor: ExtractHandler,
        db: PostgresResource,
    ):
        gdf = geoadmin.download_all_sources(
            collection=geo_config.collection,
            feature_id_regex=geo_config.feature_id_regex,
            artefact_type=geo_config.artefact_type,
            asset_key_regex=geo_config.asset_key_regex,
            layer=geo_config.layer,
        )

        # Lowercase all column names
        gdf.columns = gdf.columns.str.lower()

        # Drop duplicages
        gdf = gdf.drop_duplicates(subset=['duedate', 'gem_no'], keep='last')

        # Write to database
        gdf.to_postgis(
            f'swisstopo_{geo_config.name}',
            db.get_sqlalchemy_engine(),
            schema='src',
            if_exists='replace',
            index=False,
        )

        # Store extract via extractor
        execution_date = dt.datetime.now(tz=timezone('Europe/Zurich'))
        key = '/'.join(
            [
                f'swisstopo_{geo_config.name}',
                f'extracted_data_{execution_date.isoformat()}.fernet',
            ]
        )
        extractor.write_data(key, gdf)

        # Insert metadata
        context.add_output_metadata(
            metadata={
                'num_records': len(gdf.index),
                'num_cols': len(gdf.columns),
                'preview': MetadataValue.md(gdf.head().to_markdown()),
            }
        )

    return _asset_swisstopo


# Defintions
assets_geoadmin = [swisstopo_api_factory(geo_config) for geo_config in SOURCES]


@success_hook(required_resource_keys={'healthcheck'})
def ping_healthchecks(context: HookContext):
    """
    Pings healthchecks.io to notify that the pipeline is running.
    """
    context.resources.healthcheck.ping_by_env('HC__GEOADMIN')


job_geoadmin = define_asset_job(
    name='job_geoadmin',
    selection=[f'src/swisstopo_{source.name}*' for source in SOURCES],
    hooks={ping_healthchecks},
)

schedule_geoadmin = ScheduleDefinition(
    job=job_geoadmin,
    cron_schedule="5 2 3 * *",
)
