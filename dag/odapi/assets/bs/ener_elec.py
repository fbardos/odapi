from collections.abc import Iterator

import dagster as dg
import dlt
import polars as pl
from dlt.extract.resource import DltResource

from odapi.ops.add_meta_columns import add_meta_columns
from odapi.ops.ckan_grab_from_web import ckan_grab_pipeline_factory
from odapi.ops.ckan_ingest_from_sftp import ckan_ingest_factory
from odapi.ops.odapi_translator import OdapiTranslator
from odapi.resources.ckan.ckan import CkanResource
from odapi.resources.ssh.sftp import SFTPResource

# TODO: There are more years as separate ressources (philosophy of ODAPI to make them available)
CKAN = CkanResource(
    translator=OdapiTranslator(model_name='ener_elec_bs', file_type='parquet'),
    publisher='Kanton Basel-Stadt',
    ckan_resource_id='b47e4334-1826-49cf-b3be-a7553becae72',
    dataset_url='https://opendata.swiss/de/dataset/kantonaler-stromverbrauch-netzlast',
    geom_level='canton',
    geom_id=12,  # Basel-Stadt
)

asset_web, job_web, sensor_web, partition = ckan_grab_pipeline_factory(CKAN)


@dlt.resource(
    name=CKAN.translator.model_name,
    write_disposition='replace',
)
def _resource(
    context: dg.AssetExecutionContext,
    sftp_grab: SFTPResource,
    remote_path: str,
    batch_size: int = 100_000,
) -> Iterator[pl.DataFrame]:
    with sftp_grab.connection() as conn:
        with conn.open(remote_path, 'rb') as remote_file:
            df = pl.read_parquet(remote_file)

            for idx, batch in enumerate(df.iter_slices(n_rows=batch_size)):
                yield add_meta_columns(
                    df,
                    context=context,
                    record_offset=idx * batch_size,
                    file_source=CKAN.translator.www_url_from_remote_path(remote_path),
                    publisher=CKAN.publisher,
                    geom_level=CKAN.geom_level,
                    geom_id=CKAN.geom_id,
                )


@dlt.source
def _source(
    context: dg.AssetExecutionContext,
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
