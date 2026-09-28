import datetime as dt
from dataclasses import dataclass
from io import BytesIO

import pandas as pd
from dagster import AssetExecutionContext
from dagster import AssetIn
from dagster import AssetsDefinition
from dagster import AssetSelection
from dagster import DefaultSensorStatus
from dagster import DynamicPartitionsDefinition
from dagster import HookContext
from dagster import MetadataValue
from dagster import RunRequest
from dagster import SensorDefinition
from dagster import SensorEvaluationContext
from dagster import SensorResult
from dagster import SkipReason
from dagster import asset
from dagster import define_asset_job
from dagster import failure_hook
from dagster import sensor
from dagster import success_hook
from pytz import timezone

from odapi.resources.ckan.ckan import CkanResource
from odapi.resources.ckan.ckan import OpenDataSwiss
from odapi.resources.clickhouse.clickhouse import ClickHouseResource
from odapi.resources.extract.extract_handler import ExtractHandler
from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.postgres.postgres import XcomPostgresResource
from odapi.resources.published_models import PublishedModels
from odapi.resources.ssh.sftp import SFTPResource
from odapi.resources.url import requests_info
from odapi.resources.url.csv import OpendataswissUrlResource
from odapi.resources.utils import calculate_bytes_compression
from odapi.utils.log_perf_counter import LogTime

PUBLISHED_MODELS = PublishedModels()
CKAN_RESOURCES = [
    # Rest moved to published_models.py
    CkanResource(
        model_name='bfe_minergie',
        ckan_resource_id='3ae6d523-748c-466b-8368-04569473338e',
    ),
    CkanResource(
        model_name='ktzh_gp_bevoelkerung',
        ckan_resource_id='132b6fed-d7ea-48e3-b5dc-9e63ac16b21e',
    ),
    CkanResource(
        model_name='ktzh_gp_auslaenderanteil',
        ckan_resource_id='23cc674b-2eb6-4ad5-9ddf-87e86f0fb06f',
    ),
    CkanResource(
        model_name='ktzh_gp_avg_haushaltsgroesse',
        ckan_resource_id='ae3cc772-38e7-4d5f-87f2-73ad8e5d07c1',
    ),
    CkanResource(
        model_name='bl_downloads_portal',
        ckan_resource_id='18d024f0-ed45-49bb-9b4b-dfa97835f9a9',
        delimiter=';',
    ),
]
CKAN_RESOURCES.extend(list(PUBLISHED_MODELS.ckan_resources))


@success_hook(required_resource_keys={'ntfy'})
def ntfy_on_success(context: HookContext):
    context.resources.ntfy.send_success_message(context)


@failure_hook(required_resource_keys={'ntfy'})
def ntfy_on_failure(context: HookContext):
    context.resources.ntfy.send_failure_message(context)


def ckan_grab_pipeline_factory(ckan_resource: CkanResource) -> tuple:

    # ------------------------------------------------------------------------
    # Load from WEB
    # ------------------------------------------------------------------------

    @asset(
        compute_kind='python',
        group_name='src_opendataswiss',
        key=['src', f'download_{ckan_resource.model_name}'],
        pool='opendataswiss',
    )
    def _asset_from_web(
        context: AssetExecutionContext,
        data_opendataswiss: OpendataswissUrlResource,
        sftp_grab: SFTPResource,
        opendata_swiss: OpenDataSwiss,
        xcom: XcomPostgresResource,
        # TODO: maybe add later RequestsInfo
    ) -> None:
        if ckan_resource.file_type == 'csv':
            needs_compression = True
        else:
            needs_compression = False

        t = LogTime(context)
        time = dt.datetime.now(dt.UTC)
        data_url = opendata_swiss.get_resource_url(ckan_resource.ckan_resource_id)
        with t.step('download_data'):
            data = data_opendataswiss._get_raw_bytes(data_url)
        if needs_compression:
            with t.step('compress_data'):
                compressed = sftp_grab.compress_to_xz(data)

            # calculate metadata
            size_decompressed = data.getbuffer().nbytes
            size_compressed = compressed.getbuffer().nbytes
            size_ratio = (
                size_compressed / size_decompressed if size_decompressed else 0.0
            )
            size_pct = size_ratio * 100
            context.add_output_metadata(
                metadata={
                    "decompressed_size_bytes": size_decompressed,
                    "compressed_size_bytes": size_compressed,
                    "compression_ratio": round(size_ratio, 4),  # e.g. 0.1372
                    "compressed_vs_decompressed": f"{size_compressed}/{size_decompressed} ({size_pct:.2f}%)",
                    "space_saved_bytes": size_decompressed - size_compressed,
                    "space_saved_percent": (
                        round((1 - size_ratio) * 100, 2) if size_decompressed else 0.0
                    ),
                }
            )
        else:
            compressed = data

        # write to sftp
        # TODO: Do only upload when checksum of file has changed
        # Some data provider do update the resource updated_at every day, even
        # when no new data has arrived.
        with sftp_grab.connection() as conn:
            with t.step('ensure_sftp_dir'):
                conn.ensure_dir(ckan_resource.dir)
            with t.step('upload_sftp_file'):
                filetype = ckan_resource.filetype_ending
                compression = '.xz' if needs_compression else ''
                conn.write_file(
                    ckan_resource.path(time, f'{filetype}{compression}'),
                    compressed.getvalue(),
                )
        xcom.xcom_push(f'last_execution_{ckan_resource.model_name}', time.isoformat())

    _file_partition = DynamicPartitionsDefinition(name=ckan_resource.partition_name)

    _job_from_web = define_asset_job(
        name=ckan_resource.job_name_web,
        selection=[_asset_from_web],
        hooks={ntfy_on_success, ntfy_on_failure},
    )

    @sensor(
        job=_job_from_web,
        name=ckan_resource.sensor_name_web,
        minimum_interval_seconds=60 * 60,
    )
    def _sensor_web(
        opendata_swiss: OpenDataSwiss,
        xcom: XcomPostgresResource,
    ):
        last_execution_str = xcom.xcom_pull(
            f'last_execution_{ckan_resource.model_name}'
        )
        if last_execution_str is None:
            last_execution = dt.datetime(1970, 1, 1, tzinfo=timezone('Europe/Zurich'))
        else:
            assert isinstance(last_execution_str, str)
            last_execution = dt.datetime.fromisoformat(last_execution_str)

        last_modified = opendata_swiss.get_resource_modified(
            ckan_resource.ckan_resource_id
        )
        assert isinstance(last_modified, dt.datetime)

        if last_modified > dt.datetime.now(tz=dt.timezone.utc):
            yield SkipReason(
                'Result: '
                f'last modified {last_modified} is set in the future. '
                'Skip.'
            )
        elif last_modified > last_execution:
            yield RunRequest(job_name=ckan_resource.job_name_web)
        else:
            yield SkipReason(
                'Result: '
                f'last execution: {last_execution} > last modified {last_modified}. '
                'Skip.'
            )

    return _asset_from_web, _job_from_web, _sensor_web, _file_partition
