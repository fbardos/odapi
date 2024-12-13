import logging
import tqdm
import urllib
import pandas as pd
import requests
from typing import List
from typing import Tuple
from dagster import asset
from odapi.resources.postgres.postgres import PostgresResource
from dagster import AssetExecutionContext
from dagster import MetadataValue
from odapi.resources.extract.extract_handler import ExtractHandler
import datetime as dt
from pytz import timezone
from dataclasses import dataclass
from dagster import define_asset_job
from dagster import get_dagster_logger
from dagster import ScheduleDefinition
import re
import jq
import json


@asset(
    compute_kind='python',
    group_name='src_bfs',
    key=['src', 'bfs_statatlas'],
)
def bfs_statatlas(
    context: AssetExecutionContext,
    extractor: ExtractHandler,
    db: PostgresResource
):
    """

    During a map visit from the browser, the following URLs get called to obtain JSON:

      - https://www.atlas.bfs.admin.ch/json/13/patterns/{mapid}.json
        Contains: a string with concatenated IDs
      - https://www.atlas.bfs.admin.ch/json/13/maps/{mapid}.json
        Contains: metadata about the map
      - https://www.atlas.bfs.admin.ch/json/13/data/{mapid}.json
        Contains: data from the map

    Here is an example for the pattern for map ID 21895:

      > ["21895_13575_9079_9075_138_2857"]

    Sometimes, multiple variables get named the same, for example, the econimic
    maps have the same variable inside the CSV for Detailhandel and Maschinenbau:

      - Detailhandel, 2022, Kantone:
        https://www.atlas.bfs.admin.ch/maps/13/de/18030_9084_9075_138/27833.html
      - Maschinenbau, 2022, Kantone:
        https://www.atlas.bfs.admin.ch/maps/13/de/18031_9086_9075_138/27835.html

    Both have the variable: Anzahl Beschäftigte in Vollzeitäquivalenten
    So, simply by iterating over the map IDs, this would lead to duplicate rows when
    when following the surrogate key. To better handle this, I need:

      - A better source for the variable name.
      - A reliable ID which collects all years of one variable.

    Variable Name:
    There could be a source for a better variable name, in the maps/ endpoint, but
    I do not know how stable these are. Additionally, they contain a number for year.
    Maybe, a simple mapping in DBT would be enough to remap the name of the variables
    which share the same name string.

    Variable ID:
    It looks like when calling the patterns/ url from above, i get a reliable grouping
    by the third value (9079 in the example). This makes it possible to compare multiple
    years together.

    """
    logger = get_dagster_logger()

    @dataclass
    class StructureItem:
        structure_id: int
        structure_name_de: str
        child_id: int
        child_name_de: str

    @dataclass(eq=True, frozen=True)
    class MapItem:
        map_id: int
        structure_id: int

    def _load_structure(structure_id: str = '2857', max_depth: int = 4) -> Tuple[List[StructureItem], List[MapItem]]:
        return_structures = []
        return_maps = []

        if max_depth > 0:
            url = f'https://www.atlas.bfs.admin.ch/json/13/structures/{structure_id}.json'
            try:
                logger.info(f'Recursively loading structure from URL: {url}')
                response = requests.get(url)
            except urllib.error.HTTPError:
                logger.error(f'Failed to load structure from URL: {url}')
                return return_structures, return_maps
            try:
                response_json = json.loads(response.content)
            except json.decoder.JSONDecodeError:
                logger.error(f'Failed to parse JSON from URL: {url}')
                return return_structures, return_maps
            _structure_id = jq.compile('.ida').input(response_json).first()
            _structure_name_de = jq.compile('.translation."111"').input(response_json).first()

            # If the response has childrens, recursively call the function, otherwise, return maps.
            if len(jq.compile('.children.[]').input(response_json).all()) > 0:
                _children_raw = jq.compile('.children | keys[]').input(response_json).all()
                for raw_key in _children_raw:
                    match = re.match(r'_(\d+)', raw_key)
                    if match:
                        return_structures.append(StructureItem(
                            structure_id=int(_structure_id),
                            structure_name_de=_structure_name_de,
                            child_id=int(match.group(1)),
                            child_name_de=jq.compile(f'.children."_{match.group(1)}"."111"').input(response_json).first(),
                        ))
                        structures, maps = _load_structure(match.group(1), max_depth=max_depth - 1)
                        return_structures.extend(structures)
                        return_maps.extend(maps)

            # If the response has maps, collect them.
            if len(jq.compile('.info.[]').input(response_json).all()) > 0:
                for map in jq.compile('.info | .[].MAP').input(response_json).all():
                    return_maps.append(MapItem(
                        map_id=int(map),
                        structure_id=int(_structure_id),
                    ))

        return return_structures, return_maps

    def _expand_mother_child_structure_relations(
        data: pd.DataFrame,
        structures: List[StructureItem],
        maps: List[MapItem]
    ) -> pd.DataFrame:

        # Build hierarcy of structures, starting with maps
        df_structures = pd.DataFrame(structures).drop_duplicates()
        assert isinstance(df_structures, pd.DataFrame)
        df_maps = pd.DataFrame(maps).drop_duplicates()
        assert isinstance(df_maps, pd.DataFrame)
        max_depth = 6
        for i in range(max_depth):
            df_maps = df_maps.merge(df_structures[['child_id', 'child_name_de', 'structure_id']], left_on='structure_id', right_on='child_id', how='left')

            # Make the joined child_id the new structure_id, to be joined in the next iteration
            df_maps['structure_id'] = df_maps['structure_id_y']
            df_maps.drop(columns=['structure_id_x', 'structure_id_y'], errors='ignore', inplace=True)

            df_maps.rename(inplace=True, columns={
                'child_id': f'mother_{i}_id',
                'child_name_de': f'mother_{i}_name',
            })

            # Break if no more mothers are found
            if df_maps[f'mother_{i}_id'].isnull().all():
                df_maps.drop(columns=[f'mother_{i}_id', f'mother_{i}_name'], inplace=True)
                break

        # Join hierarchy to the data table
        return data.merge(df_maps, left_on='MAP_ID', right_on='map_id', how='left')

    dataframes = []
    skips = 0
    max_found = 0

    # Load available maps by examining the structure
    structures, maps = _load_structure(max_depth=7)
    unique_maps = list(set(maps))

    for map in tqdm.tqdm(unique_maps, mininterval=10, desc='Loading asset dataframes...'):
        try:
            df = pd.read_csv(f'https://www.atlas.bfs.admin.ch/core/projects/13/xshared/csv/{map.map_id}_131.csv', sep=';')
            df = _expand_mother_child_structure_relations(df, structures, maps)
            df['low_level_structure_id'] = map.structure_id
            dataframes.append(df)
            max_found = map.map_id
        except urllib.error.HTTPError:
            skips += 1
            continue

    logging.info(f'Loaded dataframes, skipped {skips} assets, highest ID found {max_found}.')
    df = pd.concat(dataframes)

    # Remove duplicates
    df = df.drop_duplicates()
    assert isinstance(df, pd.DataFrame)

    # Set dtypes manually
    df['GEO_ID'] = df['GEO_ID'].astype(str)

    # Write to database
    df.to_sql('bfs_statatlas', db.get_sqlalchemy_engine(), schema='src', if_exists='replace', index=False)

    # Store extract via extractor
    execution_date = dt.datetime.now(tz=timezone('Europe/Zurich'))
    key = '/'.join(['bfs_statatlas', f'extracted_data_{execution_date.isoformat()}.fernet'])
    extractor.write_data(key, df)

    # Insert metadata
    context.add_output_metadata(metadata={
        'max_found_index': max_found,
        'amount_of_skips': skips,
        'num_records': len(df.index),
        'num_cols': len(df.columns),
        'preview': MetadataValue.md(df.head().to_markdown()),
    })


job_statatlas = define_asset_job(
    name='bfs_statatlas',
    selection='src/bfs_statatlas*'
)

schedule_statatlas = ScheduleDefinition(
    job=job_statatlas,
    cron_schedule='5 2 * * 6',  # once per week, on Saturday at 02:05
)
