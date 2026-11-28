from typing import Any

import dagster as dg
import dlt
from dagster import DynamicPartitionsDefinition

from odapi.ops.hooks import ntfy_on_failure
from odapi.ops.hooks import ntfy_on_success
from odapi.ops.sftp_file_sensor import sftp_file_sensor_factory
from odapi.resources.ckan.ckan import CkanResource
from odapi.resources.ssh.sftp import SFTPResource


def ckan_ingest_factory(
    ckan: CkanResource, partition: DynamicPartitionsDefinition, dlt_source: Any
) -> tuple:

    dlt_pipeline = dlt.pipeline(
        pipeline_name=ckan.translator.model_name,
        destination='clickhouse',
        # no destination for Clickhouse, because no schema exists
    )

    @dg.asset(
        name=f'dlt_{ckan.translator.model_name}',
        partitions_def=partition,
        kinds={'dlt', 'clickhouse'},
    )
    def _asset(
        context: dg.AssetExecutionContext,
        sftp_grab: SFTPResource,
    ) -> dg.MaterializeResult:
        remote_path = '/'.join([ckan.translator.dir, context.partition_key])

        source = dlt_source(
            context=context,
            sftp_grab=sftp_grab,
            remote_path=remote_path,
        )
        load_info = dlt_pipeline.run(source)
        return dg.MaterializeResult(metadata={'dlt_load_info': str(load_info)})

    job = dg.define_asset_job(
        name=ckan.translator.model_name,
        selection=dg.AssetSelection.assets(
            _asset,
        ).downstream(),
        hooks={ntfy_on_success, ntfy_on_failure},
    )

    sensor = sftp_file_sensor_factory(ckan.translator, job, partition)

    return dlt_pipeline, _asset, sensor
