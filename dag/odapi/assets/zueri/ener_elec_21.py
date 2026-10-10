import lzma
from collections.abc import Iterator
from collections.abc import Sequence
from pathlib import Path
from tempfile import NamedTemporaryFile

import dagster as dg
import dlt
import paramiko
import polars as pl
import pyarrow as pa
from dagster import AssetExecutionContext
from dagster import DefaultSensorStatus
from dagster import RunRequest
from dagster import SensorEvaluationContext
from dagster import SensorResult
from dagster import SkipReason
from dagster_dlt import DagsterDltResource
from dagster_dlt import DagsterDltTranslator
from dagster_dlt import dlt_assets
from dagster_dlt.dlt_event_iterator import DltEventType
from dlt.extract.resource import DltResource
from dlt.sources.helpers.transform import add_row_hash_to_table

from odapi.ops.add_meta_columns import add_meta_columns
from odapi.ops.ckan_grab_from_web import ckan_grab_pipeline_factory
from odapi.ops.ckan_ingest_from_sftp import ckan_ingest_factory
from odapi.resources.ckan.ckan import CkanResource
from odapi.resources.ssh.sftp import SFTPResource

# TODO: There are more years as separate ressources (philosophy of ODAPI to make them available)
CKAN = CkanResource(
    publisher='Gemeinde Zürich',
    model_name='ener_elec_zueri_21',
    ckan_resource_id='7d8b0bf5-1366-44d5-a22d-f2c33551d86c',
    dataset_url='https://opendata.swiss/de/dataset/viertelstundenwerte-zum-bruttolastgang-elektrische-energie-der-stadt-zurich1',
    geom_level='municipality',
    geom_id=261  # Zurich
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
            with lzma.LZMAFile(remote_file, mode='rb') as decompressed_file:
                reader = pl.read_csv_batched(
                    decompressed_file,
                    separator=',',
                    encoding='utf8',
                    batch_size=batch_size,
                    infer_schema_length=10_000,
                )

                while batches := reader.next_batches(1):
                    for idx, dataframe in enumerate(batches):
                        dataframe = dataframe.lazy().collect()
                        yield add_meta_columns(
                            dataframe,
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
