import dagster as dg
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from odapi.ops.odapi_translator import OdapiTranslator
from odapi.resources.ssh.sftp import SFTPResource


def sftp_file_sensor_factory(
    translator: OdapiTranslator,
    job: UnresolvedAssetJobDefinition,
    partition: dg.DynamicPartitionsDefinition,
) -> dg.SensorDefinition:

    @dg.sensor(
        job=job,
        name=translator.sensor_name_sftp,
        required_resource_keys={'sftp_grab'},
        # Default should be STOPPED, because, when merging assets
        # from multiple Jobs in PROD, the runs will fail, until
        # the execution of the last job, because data got already
        # downloaded on DEV.
        default_status=dg.DefaultSensorStatus.STOPPED,
        minimum_interval_seconds=2 * 60,
    )
    def _sensor_sftp(
        context: dg.SensorEvaluationContext,
    ) -> dg.SensorResult | dg.SkipReason:
        sftp: SFTPResource = context.resources.sftp_grab

        with sftp.connection() as conn:
            try:
                files = conn.list_files(translator.dir)
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

    return _sensor_sftp
