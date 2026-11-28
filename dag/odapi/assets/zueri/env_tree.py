import datetime as dt
from io import BytesIO
from typing import Any
from typing import Iterator
from typing import cast

import dagster as dg
import dlt
import polars as pl
import shapely
from dlt.extract.resource import DltResource
from pyproj import Transformer

from odapi.ops.add_meta_columns import add_meta_columns
from odapi.ops.hooks import ntfy_on_failure
from odapi.ops.hooks import ntfy_on_success
from odapi.ops.odapi_translator import OdapiTranslator
from odapi.ops.sftp_file_sensor import sftp_file_sensor_factory
from odapi.resources.geo.wfs import WFSResource
from odapi.resources.ssh.sftp import SFTPResource
from odapi.utils.log_perf_counter import LogTime

translator = OdapiTranslator(
    model_name='env_tree_zueri', file_type='parquet', dir_name='wfs'
)
WFS_URL = 'https://www.ogd.stadt-zuerich.ch/wfs/geoportal/Baumkataster'
WFS_LAYER = 'baumkataster_baumstandorte'
PUBLISHER = 'Stadt Zürich'


# LOAD FROM WEB ------------------------------------------------------------------------
@dg.asset(
    compute_kind='python',
    group_name='src_ogc',
    key=['src', translator.model_name],
)
def asset_load_from_web(
    context: dg.AssetExecutionContext,
    wfs: WFSResource,
    sftp_grab: SFTPResource,
) -> None:

    gdf = wfs.load_layer(url=WFS_URL, layer=WFS_LAYER)
    buffer = BytesIO()
    gdf.to_parquet(buffer, compression='zstd', compression_level=22, index=False)

    # write to sftp
    t = LogTime(context)
    time = dt.datetime.now(dt.UTC)
    remote_dir = '/'.join([wfs.SFTP_DIR_PARENT, translator.model_name])
    with sftp_grab.connection() as conn:
        with t.step('ensure_sftp_dir'):
            conn.ensure_dir(remote_dir)
        with t.step('check_if_file_does_differ'):
            file_has_changed = sftp_grab.differs_from_latest(
                context=context,
                sftp_session=conn,
                data=buffer,
                remote_dir=remote_dir,
            )
        with t.step('upload_sftp_file'):
            if file_has_changed:
                context.log.info('File does differ. Upload new file.')
                context.add_output_metadata({'file_has_changed': 1})
                conn.write_file(
                    translator.path(time, translator.filetype_ending),
                    buffer.getvalue(),
                )
            else:
                context.add_output_metadata({'file_has_changed': 0})
                context.log.info(
                    'File has not changed from the already uploadet one. ' 'Skip upload'
                )


file_partition = dg.DynamicPartitionsDefinition(name=translator.partition_name)

job_from_web = dg.define_asset_job(
    name=translator.job_name_web,
    selection=[asset_load_from_web],
    hooks={ntfy_on_success, ntfy_on_failure},
)

schedule = dg.ScheduleDefinition(
    job=job_from_web,
    cron_schedule='15 2 * * 1',
)


# DLT LOAD -----------------------------------------------------------------------------
@dlt.resource(
    name=translator.model_name,
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
            transformer = Transformer.from_crs(2056, 4326, always_xy=True)
            df = df.with_columns(
                geometry=pl.Series(
                    shapely.to_wkb(
                        shapely.transform(
                            shapely.from_wkb(df["geometry"].to_numpy()),
                            cast(Any, transformer.transform),
                            interleaved=False,
                        )
                    )
                )
            )

            for idx, batch in enumerate(df.iter_slices(n_rows=batch_size)):
                yield add_meta_columns(
                    batch,
                    context=context,
                    record_offset=idx * batch_size,
                    file_source=translator.www_url_from_remote_path(remote_path),
                    publisher=PUBLISHER,
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


dlt_pipeline = dlt.pipeline(
    pipeline_name=translator.model_name,
    destination='clickhouse',
    # no destination for Clickhouse, because no schema exists
)


@dg.asset(
    name=f'dlt_{translator.model_name}',
    partitions_def=file_partition,
    kinds={'dlt', 'clickhouse'},
)
def _asset(
    context: dg.AssetExecutionContext,
    sftp_grab: SFTPResource,
) -> dg.MaterializeResult:
    remote_path = '/'.join([translator.dir, context.partition_key])

    source = _source(
        context=context,
        sftp_grab=sftp_grab,
        remote_path=remote_path,
    )
    load_info = dlt_pipeline.run(source)
    return dg.MaterializeResult(metadata={'dlt_load_info': str(load_info)})


job = dg.define_asset_job(
    name=translator.model_name,
    selection=dg.AssetSelection.assets(
        _asset,
    ).downstream(),
    hooks={ntfy_on_success, ntfy_on_failure},
)

sensor = sftp_file_sensor_factory(translator, job, file_partition)

# DEBUG --------------------------------------------------------------------------------
# For debugging...
# Execute with
#   uv run python dag/odapi/assets/winti/env_tree.py
if __name__ == '__main__':
    from odapi.definitions import defs

    dg.materialize([asset_load_from_web], resources=defs.resources)
