import lzma
from collections.abc import Iterator
from collections.abc import Sequence
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import dagster as dg
import dlt
import paramiko
import polars as pl
import pyarrow as pa
from dagster import DefaultSensorStatus
from dagster import DynamicPartitionsDefinition
from dagster import RunRequest
from dagster import SensorEvaluationContext
from dagster import SensorResult
from dagster import SkipReason
from dagster_dlt import DagsterDltResource
from dagster_dlt import DagsterDltTranslator
from dagster_dlt import dlt_assets
from dagster_dlt.dlt_event_iterator import DltEventType
from dlt.extract.resource import DltResource
from dlt.extract.source import DltSource
from dlt.sources.helpers.transform import add_row_hash_to_table

from odapi.ops.ckan_grab_from_web import ckan_grab_pipeline_factory
from odapi.resources.ckan.ckan import CkanResource
from odapi.resources.ssh.sftp import SFTPResource


def ckan_ingest_factory(
    ckan: CkanResource, partition: DynamicPartitionsDefinition, dlt_source: Any
) -> tuple:

    dlt_pipeline = dlt.pipeline(
        pipeline_name=ckan.model_name,
        destination='clickhouse',
        # no destination for Clickhouse, because no schema exists
    )

    @dg.asset(
        name=f'dlt_{ckan.model_name}',
        partitions_def=partition,
        kinds={'dlt', 'clickhouse'},
    )
    def _asset(
        context: dg.AssetExecutionContext,
        sftp_grab: SFTPResource,
    ) -> dg.MaterializeResult:
        remote_path = '/'.join([ckan.dir, context.partition_key])

        source = dlt_source(
            context=context,
            sftp_grab=sftp_grab,
            remote_path=remote_path,
        )
        load_info = dlt_pipeline.run(source)
        return dg.MaterializeResult(metadata={'dlt_load_info': str(load_info)})

    job = dg.define_asset_job(
        name=ckan.model_name,
        selection=dg.AssetSelection.assets(
            _asset,
        ).downstream(),
    )

    @dg.sensor(
        job=job,
        name=ckan.sensor_name_sftp,
        required_resource_keys={'sftp_grab'},
        default_status=dg.DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=2 * 60,
    )
    def _sensor_sftp(
        context: dg.SensorEvaluationContext,
    ) -> dg.SensorResult | dg.SkipReason:
        sftp: SFTPResource = context.resources.sftp_grab

        with sftp.connection() as conn:
            try:
                files = conn.list_files(ckan.dir)
            except FileNotFoundError:
                return dg.SkipReason('No files found on target. Skip.')

        # This order is important for the correct execution order for the downstream
        # snapshot (SCD-Type2).
        files = sorted(files)

        if not files:
            return dg.SkipReason('No files found on target. Skip.')

        # 1. Never start another partition while one is still running/queued.
        active_runs = context.instance.get_runs(
            filters=dg.RunsFilter(
                job_name=job.name,
                statuses=[
                    dg.DagsterRunStatus.NOT_STARTED,
                    dg.DagsterRunStatus.QUEUED,
                    dg.DagsterRunStatus.STARTING,
                    dg.DagsterRunStatus.STARTED,
                    dg.DagsterRunStatus.CANCELING,
                ],
            ),
            limit=1,
        )

        if active_runs:
            return dg.SkipReason('Previous SFTP partition is still running or queued.')

        # 2. Find partitions that actually completed successfully.
        successful_runs = context.instance.get_runs(
            filters=dg.RunsFilter(
                job_name=job.name,
                statuses=[dg.DagsterRunStatus.SUCCESS],
            ),
        )

        successful_partition_keys = {
            run.tags['dagster/partition']
            for run in successful_runs
            if 'dagster/partition' in run.tags
        }

        # 3. Take the OLDEST file that has not successfully completed.
        unprocessed_files = [
            file for file in files if file not in successful_partition_keys
        ]

        if not unprocessed_files:
            return dg.SkipReason('No new SFTP files found.')

        next_partition_key = unprocessed_files[0]

        # 4. Check whether this partition was attempted before.
        previous_runs = context.instance.get_run_records(
            filters=dg.RunsFilter(
                job_name=job.name,
                tags={
                    'dagster/partition': next_partition_key,
                },
            ),
            limit=1,
            order_by='create_timestamp',
            ascending=False,
        )

        if previous_runs:
            previous_run = previous_runs[0].dagster_run

            if previous_run.status != dg.DagsterRunStatus.SUCCESS:
                return dg.SkipReason(
                    f'Partition {next_partition_key!r} previously ended with '
                    f'status {previous_run.status.value}. '
                    'Not processing newer files.'
                )

        # 5. The dynamic partition might already exist even though its run did
        #    not successfully complete.
        existing_partition_keys = set(
            context.instance.get_dynamic_partitions(partition.name)
        )

        dynamic_partition_requests = []

        if next_partition_key not in existing_partition_keys:
            dynamic_partition_requests.append(
                partition.build_add_request([next_partition_key])
            )

        # 6. Launch exactly ONE partition.
        return dg.SensorResult(
            dynamic_partitions_requests=dynamic_partition_requests,
            run_requests=[
                dg.RunRequest(
                    partition_key=next_partition_key,
                    tags={
                        'sftp_serial': 'true',
                    },
                )
            ],
        )

    return dlt_pipeline, _asset, _sensor_sftp
