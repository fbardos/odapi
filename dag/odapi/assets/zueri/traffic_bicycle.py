from collections.abc import Iterator

import dagster as dg
import dlt
import polars as pl
from dlt.extract.resource import DltResource
from pyproj import Transformer

from odapi.ops.add_meta_columns import add_meta_columns
from odapi.ops.ckan_grab_from_web import ckan_grab_pipeline_factory
from odapi.ops.ckan_ingest_from_sftp import ckan_ingest_factory
from odapi.ops.odapi_translator import OdapiTranslator
from odapi.resources.ckan.ckan import CkanResource
from odapi.resources.ssh.sftp import SFTPResource

CKAN = CkanResource(
    translator=OdapiTranslator(model_name='traffic_bicycle_zueri', file_type='parquet'),
    publisher='Gemeinde Zürich',
    ckan_resource_id='ae513ca8-edf4-4b94-8611-bb70b5cfa09c',
    dataset_url='https://opendata.swiss/de/dataset/daten-der-automatischen-fussganger-und-velozahlung-viertelstundenwerte',
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

            # Clickhouse currently has no possibility to transform geometries
            # to another CRS. So, this must be already done here.
            # Default CRS in Clickhouse by convention: EPSG:4326
            transformer = Transformer.from_crs('EPSG:2056', 'EPSG:4326', always_xy=True)
            e, n = transformer.transform(
                df['OST'].to_numpy(),
                df['NORD'].to_numpy(),
            )
            df = df.with_columns(pl.Series('lon', e), pl.Series('lat', n)).drop(
                'OST', 'NORD'
            )

            for idx, batch in enumerate(df.iter_slices(n_rows=batch_size)):
                yield add_meta_columns(
                    batch,
                    context=context,
                    record_offset=idx * batch_size,
                    file_source=CKAN.translator.www_url_from_remote_path(remote_path),
                    publisher=CKAN.publisher,
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
