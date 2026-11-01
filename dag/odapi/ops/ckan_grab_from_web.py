import datetime as dt

import dagster as dg

from odapi.ops.hooks import ntfy_on_failure
from odapi.ops.hooks import ntfy_on_success
from odapi.ops.odapi_translator import OdapiTranslator
from odapi.resources.ckan.ckan import CkanResource
from odapi.resources.ckan.ckan import OpenDataSwiss
from odapi.resources.published_models import PublishedModels
from odapi.resources.ssh.sftp import SFTPResource
from odapi.resources.url.csv import OpendataswissUrlResource
from odapi.utils.log_perf_counter import LogTime

PUBLISHED_MODELS = PublishedModels()
CKAN_RESOURCES = [
    # Rest moved to published_models.py
    CkanResource(
        translator=OdapiTranslator(model_name='bfe_minergie'),
        ckan_resource_id='3ae6d523-748c-466b-8368-04569473338e',
    ),
    CkanResource(
        translator=OdapiTranslator(model_name='ktzh_gp_bevoelkerung'),
        ckan_resource_id='132b6fed-d7ea-48e3-b5dc-9e63ac16b21e',
    ),
    CkanResource(
        translator=OdapiTranslator(model_name='ktzh_gp_auslaenderanteil'),
        ckan_resource_id='23cc674b-2eb6-4ad5-9ddf-87e86f0fb06f',
    ),
    CkanResource(
        translator=OdapiTranslator(model_name='ktzh_gp_avg_haushaltsgroesse'),
        ckan_resource_id='ae3cc772-38e7-4d5f-87f2-73ad8e5d07c1',
    ),
    CkanResource(
        translator=OdapiTranslator(model_name='bl_downloads_portal'),
        ckan_resource_id='18d024f0-ed45-49bb-9b4b-dfa97835f9a9',
        delimiter=';',
    ),
]
CKAN_RESOURCES.extend(list(PUBLISHED_MODELS.ckan_resources))


def ckan_grab_pipeline_factory(ckan_resource: CkanResource) -> tuple:

    # ------------------------------------------------------------------------
    # Load from WEB
    # ------------------------------------------------------------------------

    @dg.asset(
        compute_kind='python',
        group_name='src_opendataswiss',
        key=['src', f'download_{ckan_resource.translator.model_name}'],
        pool='opendataswiss',
    )
    def _asset_from_web(
        context: dg.AssetExecutionContext,
        data_opendataswiss: OpendataswissUrlResource,
        sftp_grab: SFTPResource,
        opendata_swiss: OpenDataSwiss,
        # TODO: maybe add later RequestsInfo
    ) -> None:
        if ckan_resource.translator.file_type == 'csv':
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
        with sftp_grab.connection() as conn:
            with t.step('ensure_sftp_dir'):
                conn.ensure_dir(ckan_resource.translator.dir)
            with t.step('check_if_file_does_differ'):
                file_has_changed = sftp_grab.differs_from_latest(
                    context=context,
                    sftp_session=conn,
                    data=compressed,
                    remote_dir=ckan_resource.translator.dir,
                )
            with t.step('upload_sftp_file'):
                filetype = ckan_resource.translator.filetype_ending
                compression = '.xz' if needs_compression else ''
                if file_has_changed:
                    context.log.info('File does differ. Upload new file.')
                    context.add_output_metadata({'file_has_changed': 1})
                    conn.write_file(
                        ckan_resource.translator.path(time, f'{filetype}{compression}'),
                        compressed.getvalue(),
                    )
                else:
                    context.add_output_metadata({'file_has_changed': 0})
                    context.log.info(
                        'File has not changed from the already uploadet one. '
                        'Skip upload'
                    )

    _file_partition = dg.DynamicPartitionsDefinition(
        name=ckan_resource.translator.partition_name
    )

    _job_from_web = dg.define_asset_job(
        name=ckan_resource.translator.job_name_web,
        selection=[_asset_from_web],
        hooks={ntfy_on_success, ntfy_on_failure},
    )

    @dg.sensor(
        job=_job_from_web,
        name=ckan_resource.translator.sensor_name_web,
        minimum_interval_seconds=60 * 60,
    )
    def _sensor_web(
        context: dg.SensorEvaluationContext,
        opendata_swiss: OpenDataSwiss,
    ):
        if context.cursor is None or context.cursor == '':
            last_execution = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
        else:
            assert isinstance(context.cursor, str)
            last_execution = dt.datetime.fromisoformat(context.cursor)

        last_modified = opendata_swiss.get_resource_modified(
            ckan_resource.ckan_resource_id
        )
        assert isinstance(last_modified, dt.datetime)

        if last_modified > dt.datetime.now(tz=dt.timezone.utc):
            yield dg.SkipReason(
                'Result: '
                f'last modified {last_modified} is set in the future. '
                'Skip.'
            )
        elif last_modified > last_execution:
            sensor_checktime = dt.datetime.now(tz=dt.timezone.utc)
            context.update_cursor(sensor_checktime.isoformat())
            yield dg.RunRequest(job_name=ckan_resource.translator.job_name_web)
        else:
            yield dg.SkipReason(
                'Result: '
                f'last execution: {last_execution} > last modified {last_modified}. '
                'Skip.'
            )

    return _asset_from_web, _job_from_web, _sensor_web, _file_partition
