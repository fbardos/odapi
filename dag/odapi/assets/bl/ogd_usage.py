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

# TODO: There are more years as separate ressources (philosophy of ODAPI to make them available)
CKAN = CkanResource(
    publisher='Kanton Basel-Land',
    model_name='ogd_usage_bl',
    ckan_resource_id='1e3b7c5d-3f4a-4afa-979e-8c15cf70367e',
    file_type='parquet',
    dataset_url='https://opendata.swiss/de/dataset/ogd-portal-tagliche-nutzung-nach-datensatz-seit-januar-2024',
    geom_level='canton',
    geom_id=13  # Basel-Land
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

            # set fixed column names, there is a bug in the naming
            # of the columns (currently mixed up, will possibily be fixed in the future)
            df.columns = [
                'date',
                'dataset_title',
                'dataset_id',
                'visitors',
                'interactions',
            ]

            for idx, batch in enumerate(df.iter_slices(n_rows=batch_size)):
                yield add_meta_columns(
                    df,
                    context=context,
                    record_offset=idx * batch_size,
                    file_source=CKAN.www_url_from_remote_path(remote_path),
                    publisher=CKAN.publisher,
                    geom_level=CKAN.geom_level,
                    geom_id=CKAN.geom_id,
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
