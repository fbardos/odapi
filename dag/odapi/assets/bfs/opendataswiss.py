import datetime as dt
from dataclasses import dataclass

import pandas as pd
from dagster import AssetExecutionContext
from dagster import AssetIn
from dagster import AssetsDefinition
from dagster import HookContext
from dagster import MetadataValue
from dagster import RunRequest
from dagster import SensorDefinition
from dagster import asset
from dagster import define_asset_job
from dagster import failure_hook
from dagster import sensor
from pytz import timezone

from odapi.resources.ckan.ckan import OpenDataSwiss
from odapi.resources.extract.extract_handler import ExtractHandler
from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.postgres.postgres import XcomPostgresResource
from odapi.resources.url.csv import OpendataswissUrlResource
from odapi.resources.utils import calculate_bytes_compression


@dataclass
class CkanResource:
    model_name: str
    ckan_resource_id: str


CKAN_RESOURCES = [
    CkanResource(model_name='bfe_minergie',                     ckan_resource_id='3ae6d523-748c-466b-8368-04569473338e'),
    CkanResource(model_name='ktzh_gp_bevoelkerung',             ckan_resource_id='132b6fed-d7ea-48e3-b5dc-9e63ac16b21e'),
    CkanResource(model_name='ktzh_gp_auslaenderanteil',         ckan_resource_id='23cc674b-2eb6-4ad5-9ddf-87e86f0fb06f'),
    CkanResource(model_name='ktzh_gp_avg_haushaltsgroesse',     ckan_resource_id='ae3cc772-38e7-4d5f-87f2-73ad8e5d07c1'),
]


def opendataswiss_extract_factory(ckan_resource: CkanResource) -> AssetsDefinition:

    @asset(compute_kind='python', group_name='src_opendataswiss', key=f'extract_{ckan_resource.model_name}')
    def _asset(
            context: AssetExecutionContext,
            data_opendataswiss: OpendataswissUrlResource,
            xcom: XcomPostgresResource,
            extractor: ExtractHandler,
            opendata_swiss: OpenDataSwiss,
        ) -> str:
        execution_date = dt.datetime.now(tz=timezone('Europe/Zurich'))
        key = '/'.join([ckan_resource.model_name, f'extracted_data_{execution_date.isoformat()}.fernet'])

        # Load data from CKAN
        df, byte_size = data_opendataswiss.load_data(opendata_swiss.get_resource_url(ckan_resource.ckan_resource_id))
        assert isinstance(df, pd.DataFrame)

        # Execute the extractor
        compressed_size = extractor.write_data(key, df)

        # Store last execution time to XCOM
        xcom.xcom_push(f'last_execution_{ckan_resource.model_name}', execution_date.isoformat())

        # Insert metadata
        context.add_output_metadata(metadata={
            'num_records': len(df),
            'num_cols': len(df.columns),
            'preview': MetadataValue.md(df.head().to_markdown()),
            'compression': calculate_bytes_compression(byte_size, compressed_size),
            'size_rawdata_bytes': byte_size,
            'size_compressed_bytes': compressed_size,
        })
        return key
    return _asset


def opendataswiss_load_extract_factory(asset_name: str) -> AssetsDefinition:

    @asset(
        compute_kind='python',
        group_name='src_opendataswiss',
        key=['src', asset_name],
        ins={'upstream_object_key': AssetIn(f'extract_{asset_name}')},
    )
    def _asset(
            context: AssetExecutionContext,
            extractor: ExtractHandler,
            db: PostgresResource,
            upstream_object_key
    ) -> None:
        df = extractor.read_data(upstream_object_key)
        df.to_sql(asset_name, db.get_sqlalchemy_engine(), schema='src', if_exists='replace', index=False)

        # Insert metadata
        context.add_output_metadata(metadata={
            'num_records': len(df.index),
            'num_cols': len(df.columns),
            'preview': MetadataValue.md(df.head().to_markdown()),
        })
    return _asset


# Assets
assets_opendataswiss_extract = [
    opendataswiss_extract_factory(asset)
    for asset in CKAN_RESOURCES
]
assets_opendataswiss_load_extract = [
    opendataswiss_load_extract_factory(asset.model_name)
    for asset in CKAN_RESOURCES
]


@failure_hook(required_resource_keys={'pushover'})
def pushover_on_failure(context: HookContext):
    context.resources.pushover.send_failure_message(context)

# Jobs
jobs_opendataswiss = [
    define_asset_job(
        name=asset.model_name,
        selection=[
            f'extract_{asset.model_name}',
            f'src/{asset.model_name}*',
        ],
        hooks={pushover_on_failure},
    )
    for asset in CKAN_RESOURCES
]


# Sensors
def opendataswiss_sensor_factory(job_name: str, asset_name: str, resource_id: str) -> SensorDefinition:

    @sensor(name=asset_name, job_name=job_name, minimum_interval_seconds=60 * 60)
    def _sensor(
            opendata_swiss: OpenDataSwiss,
            xcom: XcomPostgresResource,
        ):

        last_execution_str = xcom.xcom_pull(f'last_execution_{asset_name}')
        if last_execution_str is None:
            last_execution = dt.datetime(1970, 1, 1, tzinfo=timezone('Europe/Zurich'))
        else:
            assert isinstance(last_execution_str, str)
            last_execution = dt.datetime.fromisoformat(last_execution_str)

        last_modified = opendata_swiss.get_resource_modified(resource_id)
        assert isinstance(last_modified, dt.datetime)

        if last_modified > last_execution:
            yield RunRequest(run_key=job_name)

    return _sensor


sensors_opendataswiss = [
    opendataswiss_sensor_factory(
        job_name=asset.model_name,
        asset_name=asset.model_name,
        resource_id=asset.ckan_resource_id
    )
    for asset in CKAN_RESOURCES
]
