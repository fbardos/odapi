from collections.abc import Iterator

import dlt
import polars as pl
from dagster import AssetExecutionContext
from dlt.extract.resource import DltResource

from odapi.ops.add_meta_columns import add_meta_columns
from odapi.ops.ckan_grab_from_web import ckan_grab_pipeline_factory
from odapi.ops.ckan_ingest_from_sftp import ckan_ingest_factory
from odapi.resources.ckan.ckan import CkanResource
from odapi.resources.ssh.sftp import SFTPResource

CKAN = CkanResource(
    publisher='Stadt St.Gallen',
    model_name='traffic_bicycle_stgallen',
    ckan_resource_id='f8d4fdc3-3357-43de-9beb-a74c62cbfa66',
    file_type='parquet',
    dataset_url='https://opendata.swiss/de/dataset/velozahlungen-stadt-st-gallen-ab-2016',
)

asset_web, job_web, sensor_web, partition = ckan_grab_pipeline_factory(CKAN)


@dlt.resource(
    name=CKAN.model_name,
    write_disposition='replace',
)
def _resource(
    context: AssetExecutionContext,
    sftp_grab: SFTPResource,
    remote_path: str,
    batch_size: int = 100_000,
) -> Iterator[pl.DataFrame]:

    with sftp_grab.connection() as conn:
        with conn.open(remote_path, 'rb') as remote_file:
            df = pl.read_parquet(remote_file)

            for idx, batch in enumerate(df.iter_slices(n_rows=batch_size)):
                yield add_meta_columns(
                    batch,
                    context=context,
                    record_offset=idx * batch_size,
                    file_source=CKAN.www_url_from_remote_path(remote_path),
                    publisher=CKAN.publisher,
                )


@dlt.source
def _source(
    context: AssetExecutionContext,
    sftp_grab: SFTPResource,
    remote_path: str,
) -> list[DltResource]:
    return [
        _resource(
            context=context,
            sftp_grab=sftp_grab,
            remote_path=remote_path,
        ),
    ]


pipeline, asset_ingest, sensor_ingest = ckan_ingest_factory(CKAN, partition, _source)
