import datetime as dt
import gzip
import json
from collections.abc import Iterator
from typing import Any

import dagster as dg
import dlt
import requests
from dagster import BackfillPolicy
from dagster import DagsterError
from dagster import DailyPartitionsDefinition
from dagster_dlt import DagsterDltResource
from dagster_dlt import dlt_assets

PARTITION_DEF = DailyPartitionsDefinition(
    start_date=dt.datetime(2022, 8, 27), timezone='Europe/Zurich'
)


def select_file_from_web(partition: str) -> str | None:
    FILES_API_URL = 'https://files.bardos.dev/api/opendata_swiss/'
    FILES_WEB_URL = 'https://files.bardos.dev/opendata_swiss'
    files = requests.get(FILES_API_URL).json()
    selected_file = [file['name'] for file in files if partition in file['name']]
    if len(selected_file) > 0:
        return '/'.join([FILES_WEB_URL, selected_file[0]])
    else:
        return None


@dlt.resource(
    name='meta',
    primary_key='id',
    write_disposition={
        'disposition': 'merge',
        'strategy': 'scd2',
    },
)
def ndjson_records(
    snapshot_date: str | None = None,
) -> Iterator[dict[str, Any]]:
    if snapshot_date is None:
        raise ValueError('A snapshot date is required')
    url = select_file_from_web(snapshot_date)
    if url is None:
        raise DagsterError(f'No file found for partition {snapshot_date}')
    with requests.get(
        url,
        stream=True,
        timeout=(10, 180),
    ) as response:
        response.raise_for_status()
        response.raw.decode_content = False
        count = 0
        with gzip.GzipFile(fileobj=response.raw, mode='rb') as file:
            for line in file:
                if not line.strip():
                    continue
                record = json.loads(line)
                if not isinstance(record, dict) or record.get('id') is None:
                    raise ValueError('Expected a JSON object with an id')
                count += 1
                yield record
        if count == 0:
            raise ValueError(f'Empty snapshot: {url}')


@dlt.source(root_key=True, name='bfs_opendata_swiss')
def source(snapshot_date: str | None = None):
    resource = ndjson_records(snapshot_date)
    if snapshot_date is not None:
        resource.apply_hints(
            write_disposition={
                'disposition': 'merge',
                'strategy': 'scd2',
                'boundary_timestamp': f'{snapshot_date}T00:00:00+00:00',
            }
        )
    return resource


pipeline = dlt.pipeline(
    pipeline_name='pipe_bfs_opendata_swiss_meta',
    destination='clickhouse',
    dataset_name='bfs_odch_meta',
)


@dlt_assets(
    dlt_source=source(),
    dlt_pipeline=pipeline,
    name='bfs_odch_meta',
    group_name='ingest',
    partitions_def=PARTITION_DEF,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool='bfs_opendata_swiss_meta',
)
def asset(
    context: dg.AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(
        context=context,
        dlt_source=source(context.partition_key),
    )


job = dg.define_asset_job(
    name='bfs_opendata_swiss_meta',
    selection=dg.AssetSelection.assets(asset).downstream(),
    tags={'scd2_pipeline': 'bfs_odch_meta'},
)


schedule = dg.build_schedule_from_partitioned_job(
    job,
    hour_of_day=3,
    minute_of_hour=0,
)
