"""Extraction of data from the new Statatlas 2.0

List Indicators:

    Indicators can be read from the following URL:

        https://mapexplorer.bfs.admin.ch/GC_listIndics.php?lang=de

Single Indicator:

    Indicators are loaded from an URL like the following:

        https://mapexplorer.bfs.admin.ch/GC_indic.php?
            indic=stndigewohnbevlkerun
            &dataset=ch_01_02
            &view=map179
            &lang=de
            &obs=main

    Meaning of the parameters:

    - `indic`: The indicator name, e.g. `stndigewohnbevlkerun`.
        Source: GC_indic.php
    - `dataset`: The dataset name, e.g. `ch_01_02`.
        Source: GC_indic.php
    - `view`: The view name, e.g. `map179`, with the following meaning:
        map179 stands for "Politische Gemeinden 01.01.2024"
        map33 stands for "Kantone 01.01.2023"
        map164 stands for "Kantone 01.01.2022"
        Source (all available maps): GC_indic.php

Refdata:

    The data form a single indicator is returned in a sorte list.
    The column `sortIndices` does not refer to the geo_value (e.g. canton_bfs_id).
    Instead, call `GC_refdata.php` and load the bfs_id from there.

    For example, with the following URL:
        https://mapexplorer.bfs.admin.ch/GC_refdata.php?
            nivgeo=kant2022_01_01
            &extent=ch
            &lang=de
            &obs=main

Other attributes:

    - `nivgeo`: Base borders at a given point in time like kant2022_01_01.
        Nivgeos exist for e.g. kant, bezk, polk, but also for arbeitsplatzgebiete, ...
    - `map164` Describes a combination between nivgeo and a second axis,
        like "Vegetaitonsflaeche for Gemeinden"

"""

import datetime as dt
import re
from typing import List

import numpy as np
import pandas as pd
import requests
from dagster import AssetExecutionContext
from dagster import HookContext
from dagster import MetadataValue
from dagster import ScheduleDefinition
from dagster import asset
from dagster import define_asset_job
from dagster import failure_hook
from dagster import get_dagster_logger
from dagster import success_hook
from pytz import timezone
from tqdm import tqdm

from odapi.resources.extract.extract_handler import ExtractHandler
from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.url.requests_info import RequestsInfo

URL_LIST_INDICATORS = 'https://mapexplorer.bfs.admin.ch/GC_listIndics.php?lang=de'

logger = get_dagster_logger()


@asset(
    compute_kind='python',
    group_name='src_bfs',
    key=['src', 'bfs_statatlas_v2'],
)
def bfs_statatlas_v2(
    context: AssetExecutionContext,
    extractor: ExtractHandler,
    db: PostgresResource,
    requests_info: RequestsInfo,
):

    def get_indicators() -> List[dict]:
        """
        Fetch the list of indicators from the Statatlas 2.0 API.

        Returns:
            List[dict]: A list of dictionaries containing indicator information.
        """
        response = requests.get(URL_LIST_INDICATORS, headers=requests_info.headers)
        response.raise_for_status()
        return response.json().get('content').get('indics', [])

    skips = 0
    indicators = get_indicators()
    collected_dfs = []
    count_calls = 0

    # Cache the responses from refdata to not overload the server.
    collected_refdata = {}

    for indicator in tqdm(indicators):

        # Currently only use polg, bezk and kant ans nivgeo prefixes.
        # Could be extended in the future when there is a need for it.
        # If the desired nivgeo is not listed in the available nivgeos
        # on the indicator, it will not be processed.
        # Currently, it is not possible to filter for a specific nivgeo
        # (and view does change every year). So, when an indicator has
        # other nivgeos, they will be loaded too, as long as polg, bezk or kant
        # is listed in the nivgeos.
        if not any(
            nivgeo.startswith(prefix)
            for prefix in ['polg', 'bezk', 'kant']
            for nivgeo in indicator.get('nivgeos', [])
        ):
            logger.debug(
                f"Skipping indicator {indicator.get('c_id_indicateur')}: "
                "No relevant nivgeo found"
                f"Available nivgeos: {', '.join(indicator.get('nivgeos', []))}"
            )
            skips += 1
            continue

        for map_view in indicator.get('c_id_view', []):
            for year in indicator.get('mods', []):
                logger.debug(f"Processing call number: {count_calls}")
                count_calls += 1

                # At the moment, only process single year mods.
                # When other mods are observed, they will simply return
                # the same data for each year. This can be changed in the future.
                if not re.match(r'^\s*\d{4}\s*$', year):
                    logger.debug(
                        f"Skipping indicator {indicator.get('c_id_indicateur')}: "
                        f"Year {year} is not a valid year format. "
                        "Expected format: YYYY"
                    )
                    skips += 1
                    continue

                # manual generating the params string needed, because otherwise,
                # annee=2022 gets translated to annee%3D2022.
                params = {
                    'indic': indicator.get('c_id_indicateur'),
                    'dataset': indicator.get('c_id_dataset'),
                    'view': map_view,
                    'lang': 'de',
                    'obs': 'main',
                    'filters': f'annee={year}',
                }
                params_str = '&'.join(f'{key}={value}' for key, value in params.items())
                try:
                    response_indicator = requests.get(
                        'https://mapexplorer.bfs.admin.ch/GC_indic.php',
                        params=params_str,
                        headers=requests_info.headers,
                    )
                    response_indicator.raise_for_status()
                except requests.RequestException as e:
                    logger.warning(f"Error fetching indicator data: {e}")
                    skips += 1
                    continue
                response_data = response_indicator.json()
                response_distribution = response_data.get('content', {}).get(
                    'distribution', {}
                )
                response_indic = response_data.get('content', {}).get('indic', {})
                _nivgeo = response_data.get('content', {}).get('nivgeo', '')

                # Do load additional information about the geo_values, if they are
                # not already cached.
                if _nivgeo not in collected_refdata:
                    response_ref = requests.get(
                        'https://mapexplorer.bfs.admin.ch/GC_refdata.php',
                        params={
                            'nivgeo': _nivgeo,
                            'extent': 'ch',
                            'lang': 'de',
                            'obs': 'main',
                        },
                        headers=requests_info.headers,
                    )
                    collected_refdata[_nivgeo] = response_ref.json()

                # Collect the raw data
                geo_values = (
                    collected_refdata[_nivgeo]
                    .get('content', {})
                    .get('territories', {})
                    .get('codgeo', None)
                )
                geo_names = (
                    collected_refdata[_nivgeo]
                    .get('content', {})
                    .get('territories', {})
                    .get('libgeo', None)
                )
                try:
                    indicator_values = response_distribution['values']
                except KeyError:
                    logger.warning(
                        f"Indicator {indicator.get('c_id_indicateur')} "
                        f"does not have values for year {year} and map {map_view}. Skip."
                    )
                    skips += 1
                    continue

                # Some indicators list grouped values. Then the response returns a dict.
                # Examples are enterprise sizes (mikrounte, kleinunt, grossunte, ..)
                if isinstance(indicator_values, list):
                    indicator_values = {'total': indicator_values}

                for group in indicator_values:

                    try:
                        df = pd.DataFrame(
                            {
                                'geo_value': geo_values,
                                'geo_name': geo_names,
                                'indicator_value': indicator_values[group],
                            }
                        )
                    except ValueError as e:
                        logger.warning(
                            f"Error creating DataFrame for indicator {indicator.get('c_id_indicateur')}: {e}"
                        )
                        logger.debug(
                            f"geo_values: {geo_values}, "
                            f"geo_names: {geo_names}, "
                            f"indicator_values: {indicator_values[group]}"
                        )
                        skips += 1
                        continue

                    # add additional info
                    df['group_name'] = group
                    df['bfs_indicateur_id'] = response_indic['c_id_indicateur']
                    df['indicator_name'] = response_indic['c_lib_indicateur']
                    df['indicator_unit'] = response_indic['c_desc_indicateur']
                    df['source'] = response_indic['c_source']
                    df['map_id'] = response_indic['c_id_view']
                    df['bfs_nivgeo'] = _nivgeo
                    df['period_ref'] = year

                    # it seems that missing values are presented
                    # with a value of -9999.
                    df['indicator_value'] = df['indicator_value'].replace(-9999, np.nan)

                    collected_dfs.append(df)

    df_final = pd.concat(collected_dfs, ignore_index=True)
    df_final.drop_duplicates(inplace=True, ignore_index=True)

    # Add general metadata
    df_final['_source_name'] = 'bfs_statatlas_v2'
    df_final['_source_time'] = dt.datetime.now(tz=timezone('UTC'))
    df_final['_source_seqn'] = df_final.index

    # Write to database
    df_final.to_sql(
        'bfs_statatlas_v2',
        db.get_sqlalchemy_engine(),
        schema='src',
        if_exists='replace',
        index=False,
    )

    # Store export to S3
    execution_date = dt.datetime.now(tz=timezone('Europe/Zurich'))
    key = '/'.join(
        ['bfs_statatlas_v2', f'extracted_data_{execution_date.isoformat()}.fernet']
    )
    extractor.write_data(key, df_final)

    context.add_output_metadata(
        metadata={
            'amount_of_calls': count_calls,
            'amount_of_skips': skips,
            'num_records': len(df_final.index),
            'num_cols': len(df_final.columns),
            'num_distinct_indicators': df_final['bfs_indicateur_id'].nunique(),
            'num_distinct_sources': df_final['source'].nunique(),
            'preview': MetadataValue.md(df_final.head().to_markdown()),
        }
    )


@success_hook(required_resource_keys={'healthcheck'})
def ping_healthchecks(context: HookContext):
    """
    Pings healthchecks.io to notify that the pipeline is running.
    """
    context.resources.healthcheck.ping_by_env('HC__BFS_STATATLAS')


@failure_hook(required_resource_keys={'pushover'})
def pushover_on_failure(context: HookContext):
    context.resources.pushover.send_failure_message(context)


job_statatlas_v2 = define_asset_job(
    name='bfs_statatlas_v2',
    selection='src/bfs_statatlas_v2*',
    hooks={ping_healthchecks, pushover_on_failure},
)

schedule_statatlas_v2 = ScheduleDefinition(
    job=job_statatlas_v2,
    cron_schedule='5 2 * * 6',  # once per week, on Saturday at 02:05
)
