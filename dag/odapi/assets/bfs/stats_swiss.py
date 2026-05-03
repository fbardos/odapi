import datetime as dt
from dataclasses import dataclass
from enum import Enum
from io import BytesIO
from typing import Callable
from typing import List
from typing import Optional

import great_expectations as gx
import numpy as np
import pandas as pd
import sdmx
from dagster import AssetCheckResult
from dagster import AssetExecutionContext
from dagster import AssetsDefinition
from dagster import DagsterError
from dagster import HookContext
from dagster import MetadataValue
from dagster import MonthlyPartitionsDefinition
from dagster import ScheduleDefinition
from dagster import asset
from dagster import asset_check
from dagster import build_schedule_from_partitioned_job
from dagster import define_asset_job
from dagster import failure_hook
from dagster import get_dagster_logger
from dagster import success_hook
from great_expectations.expectations.core.expect_column_values_to_not_be_null import (
    ExpectColumnValuesToNotBeNull,
)
from great_expectations.expectations.core.expect_table_row_count_to_be_between import (
    ExpectTableRowCountToBeBetween,
)
from pandas import DataFrame
from sqlalchemy.dialects.postgresql import JSONB

from odapi.debug_tty import set_trace
from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.qa.great_expectations import GreatExpectationsResource
from odapi.resources.ssh.sftp import SFTPResource
from odapi.resources.url.requests_info import RequestsInfo
from odapi.resources.url.sdmx import SdmxConfig
from odapi.resources.url.sdmx import StatsSwissResource
from odapi.utils.log_perf_counter import LogTime

CONFIG = SdmxConfig(
    name='whg_gebkat',
    agency_id='CH1.GWS',
    dataflow_id='DF_GWS_REG5',
    dataflow_version='1.0.0',
    source_overwrite='Gebäude- und Wohnungsstatistik (seit 2009)',
)

MONTHLY_PARTITION = MonthlyPartitionsDefinition(
    start_date=dt.datetime(2026, 4, 1),
    # end_offset will switch from retrospective partitions to current time windows
    # Example: at 2026-05-01 a new partition (and run) will be inserted
    # with context.partition_time_window
    #   start: 2026, 5, 1
    #   end: 2026, 6, 1
    # Because we are looking from the time the exports was generated, offset = 1
    # is perfect.
    end_offset=1,
)


@asset(
    compute_kind='python',
    group_name='src_bfs',
    key=['src', f'statswiss_whg_gebkat_grab'],
    pool='stats_swiss',
)
def asset_download(
    context: AssetExecutionContext,
    stats_swiss: StatsSwissResource,
    sftp_grab: SFTPResource,
    requests_info: RequestsInfo,
) -> None:
    t = LogTime(context)
    dir = CONFIG.dir_name
    filename = f'{CONFIG.dataflow_id}_{dt.datetime.now().isoformat()}.csv.xz'
    path = '/'.join([dir, filename])
    with t.step('download_data'):
        data = stats_swiss.raw_data(CONFIG, requests_info)
    with t.step('compress_data'):
        compressed = sftp_grab.compress_to_xz(data)

    # calculate metadata
    size_decompressed = data.getbuffer().nbytes
    size_compressed = compressed.getbuffer().nbytes
    size_ratio = size_compressed / size_decompressed if size_decompressed else 0.0
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

    # write to sftp
    with t.step('ensure_sftp_dir'):
        sftp_grab.ensure_dir(CONFIG.dir_name)
    with t.step('upload_sftp_file'):
        sftp_grab.write_file(path, compressed.getvalue())


@asset(
    compute_kind='python',
    group_name='src_bfs',
    key=['src', f'statswiss_whg_gebkat'],
    pool='stats_swiss',
    partitions_def=MONTHLY_PARTITION,
    deps=[asset_download],
)
def asset_load_staging(
    context: AssetExecutionContext,
    stats_swiss: StatsSwissResource,
    db: PostgresResource,
    sftp_grab: SFTPResource,
) -> pd.DataFrame:
    t = LogTime(context)
    # # metadata: dict = stats_swiss.dataflow(context, CONFIG)
    # dataflow = CONFIG.dataflow
    # dsd = CONFIG.dsd
    # df_codelist = CONFIG.codelist_series_by_id('GBAUPS')

    # Prepare dataframes:
    #  - df_data with the indicator data
    #    (dimensions as jsonb {'group_id': 'group_value'})
    #  - df_grouping with metadata about grouping

    with t.step('look_for_files_sftp'):
        filepaths = sftp_grab.find_files_in_time_window(
            CONFIG.dir_name, context.partition_time_window
        )
        if len(filepaths) != 1:
            raise DagsterError(
                f'Expected 1 single file for partition: {context.partition_key}, '
                f'got: {filepaths}'
            )
        selected_file = filepaths[0]
    with t.step('read_compressed_file'):
        compressed = BytesIO(sftp_grab.read_file(selected_file))
    with t.step('decompress_file'):
        data = sftp_grab.decompress_from_xz(compressed)
    with t.step('process_raw_data'):
        source_system = (
            f'stats.swiss {CONFIG.agency_id}, {CONFIG.dataflow_id}, '
            f'FILE https://files.bardos.dev/odapi{selected_file}'
        )
        df = stats_swiss.process_raw_data(
            data=data,
            config=CONFIG,
            source_system=source_system,
            source_timestamp=dt.datetime.now(),
        )

    # Maybe, in the future, expand grouping, i want something like:
    #
    # {
    #    "GKATS": {
    #       "group_name": "Gebäudekategorie",
    #       "group_def": "https://i14y...",
    #       "group_value_id": 1025,
    #       "group_value_tx": "Mehrfamilienhaus",
    #       "group_value_is_total": False,
    #    },
    # }
    #
    # TODO: Or, a even better approach is to store each grouping value in a
    # long table. Because currently, every key uses space on the disk, even,
    # if group_name is repeated millions of times.
    #
    # main table: primary_key
    #
    # grouping_table:
    #
    #   data_sk     group_id    group_value
    #   a390sdd...  GKATS       1025
    #
    # group_value:
    #   group_id    group_value     description_de
    #   GKATS       1025            Mehrfamilienhaus
    #
    # group_table:
    #
    #   group_id    group_name          uri             description
    #   GKATS       Gebaeudekategorie   https://i14y    '...'
    #
    # grouping as originally introduced, should be structured only in the final API
    # response, when expand_groups = False (default)
    #
    # {"GKATS": {"1040": "Buildings with partial residential use"}, "GBAUPS": {"8020": "Period from 2001 to 2005"}, "WAZIMS": {"1": "1 room"}}
    #
    with t.step('expand_dimensions'):
        proc_dim = CONFIG.expand_dimensions(df, skip_dims=['FREQ', 'TIME_PERIOD'])
    with t.step('melt_dimensions'):
        df = CONFIG.melt_dimensions(proc_dim)

    # dict columns need to be set in dtype as JSONB
    dtypes = {CONFIG.dim_dict(i): JSONB for i in proc_dim.dimensions}
    dtypes['grouping'] = JSONB

    # Before uploading, set column names to lowercase
    # Do this here, not inside SdmxConfig or StatsSwissResource,
    # because uppercase column names get referenced there from DSD.
    df.columns = map(str.lower, df.columns)

    with t.step('insert_into_db'):
        df.to_sql(
            context.asset_key.path[-1],
            db.get_sqlalchemy_engine(),
            schema='src',
            if_exists='replace',
            index=False,
            chunksize=1_000_000,
            dtype=dtypes,
        )

    context.add_output_metadata(
        metadata={
            'num_records': len(df.index),
            'num_cols': len(df.columns),
            'preview': MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df


@asset_check(asset=asset_load_staging, blocking=True)
def gx_rows_ge_1000(
    great_expectations: GreatExpectationsResource,
    data: pd.DataFrame,
) -> AssetCheckResult:
    expectation = ExpectTableRowCountToBeBetween(
        min_value=1_000,
    )
    return great_expectations.run_expectation(data, expectation)


@asset_check(asset=asset_load_staging, blocking=True)
def gx_grouping_not_null(
    great_expectations: GreatExpectationsResource,
    data: pd.DataFrame,
) -> AssetCheckResult:
    expectation = ExpectColumnValuesToNotBeNull(
        column='grouping',
    )
    return great_expectations.run_expectation(data, expectation)


job = define_asset_job(
    name=f"load_{CONFIG.dataflow_id}",
    selection='src/statswiss_whg_gebkat_grab*',
)

schedule = build_schedule_from_partitioned_job(job)
