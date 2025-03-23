from dataclasses import dataclass
from enum import Enum
from typing import Callable
from typing import List
from typing import Optional

import numpy as np
from dagster import AssetExecutionContext
from dagster import AssetsDefinition
from dagster import DagsterError
from dagster import HookContext
from dagster import MetadataValue
from dagster import ScheduleDefinition
from dagster import asset
from dagster import define_asset_job
from dagster import failure_hook
from dagster import get_dagster_logger
from dagster import success_hook
from pandas import DataFrame

from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.url.stat_tab import StatTabResource


class GeoValueStructure(Enum):
    DOT_NOTATION = r'^(?:Schweiz|Ohne Angabe|-|>>|<<|(?:\.\.\.\.\.\.\d*))(?:.*)'
    LEADING_NUMBER = r'^(?:(?:\d+)(?:\s\w+).*|Schweiz)$'


@dataclass
class ColumnDefinition:
    name: str
    is_indicator_group: bool = False
    do_drop_before_upload: bool = False
    is_year: bool = False


@dataclass
class StatTabCube:
    name: str
    bfs_id: str
    url: str
    columns: List[ColumnDefinition]
    encoding: str = 'utf-8'
    custom_transform: Optional[Callable[[DataFrame], DataFrame]] = None

    def _rename_columns(self, df: DataFrame) -> DataFrame:
        get_dagster_logger().info(f"Renaming columns for cube {self.name}")
        df.columns = [col.name for col in self.columns]
        return df

    def _restructure_year_column(
        self, df: DataFrame, unstructured_year_column: str
    ) -> DataFrame:
        get_dagster_logger().info(f"Restructure year column")

        _col = unstructured_year_column

        # Convert to a unified format from/to, e.g. '2024/2024'
        df[_col] = df[_col].str.replace(r'^(\d{4})$', r'\1-\1', regex=True)
        df[_col] = df[_col].str.replace(
            r'^(\d{2})(\d{2})\/(\d{2})$', r'\1\2-\1\3', regex=True
        )

        # Split column
        df[['year_from', 'year_to']] = df[_col].str.split('-', n=1, expand=True)

        return df

    def _restructure_dot_notation(
        self, df: DataFrame, unstructured_geo_column: str
    ) -> DataFrame:
        """Splits the geo_value from the source in geo_code and geo_value

        Source data on municipal level contains values according to the following pattern:

            Column: Kanton (-) / Bezirk (>>) / Gemeinde (......)
            With e.g. value for Winterthur: ......0230 Winterthur

        This method will split the information in two columns: geo_code and geo_value.

        """
        get_dagster_logger().info(
            f"Extract geo info from column {unstructured_geo_column}"
        )

        _conditions = [
            df[unstructured_geo_column].str.contains(r'^-'),
            df[unstructured_geo_column].str.contains(r'^>>'),
            df[unstructured_geo_column].str.contains(r'^\.\.\.\.\.\.'),
        ]
        _results = ['kant', 'bezk', 'polg']
        df['geo_code'] = np.select(_conditions, _results, default='')

        df['geo_value'] = df[unstructured_geo_column].str.extract(
            r'^\.\.\.\.\.\.(\d+)', expand=True
        )
        df['geo_value_name'] = df[unstructured_geo_column].str.extract(
            r'^(?:-|>>|(?:\.\.\.\.\.\.\d*))(.*)', expand=True
        )
        df['geo_value_name'] = df['geo_value_name'].str.strip()
        return df

    def _restructure_leading_number(
        self, df: DataFrame, unstructured_geo_column: str
    ) -> DataFrame:
        get_dagster_logger().info(
            f"Extract geo info from column {unstructured_geo_column}"
        )
        df['geo_code'] = 'polg'
        df['geo_value'] = df[unstructured_geo_column].str.extract(
            r'^(\d+)(?:\s\w+).*', expand=True
        )
        df['geo_value_name'] = df[unstructured_geo_column].str.extract(
            r'^\d+\s(.*)', expand=True
        )
        return df

    def _count_rows_matching_regex(self, df: DataFrame, column: str, regex: str) -> int:
        return len(df[df[column].str.match(regex)].index)

    def _decide_restructure_geo_value(self, df: DataFrame, column: str) -> DataFrame:
        """Decides how to restructure the geo_value column based on the structure of the data.

        Methods called for restructuring must create these columns inside the DataFrame object:
          - geo_code
          - geo_value
          - geo_value_name

        """
        MATCH_THRESHOLD = 0.8
        _df_len = len(df.index)
        _rows_dot_notation = self._count_rows_matching_regex(
            df, column, GeoValueStructure.DOT_NOTATION.value
        )
        _rows_leading_number = self._count_rows_matching_regex(
            df, column, GeoValueStructure.LEADING_NUMBER.value
        )
        if _rows_dot_notation > _df_len * MATCH_THRESHOLD:
            return self._restructure_dot_notation(df, column)
        elif _rows_leading_number > _df_len * MATCH_THRESHOLD:
            return self._restructure_leading_number(df, column)
        else:
            _error_msg = 'Could not decide how to restructure geo_value'
            for enum in GeoValueStructure:
                _unmatched = df[~df[column].str.match(enum.value)]
                _error_msg += (
                    f"\n{enum.name} ================================================="
                )
                _error_msg += f"\n{enum.name}: {len(_unmatched.index)} of {len(df.index)} rows unmatched"
                _error_msg += f"\n{enum.name}: Data sample\n{_unmatched[column].value_counts().head(20)}"
            raise DagsterError(_error_msg)

    def _add_source(self, df: DataFrame, meta: dict) -> DataFrame:
        get_dagster_logger().info(f"Add source from metadata")
        df['source'] = ', '.join(meta.get('SOURCE', ['unknown']))
        return df

    def _add_column_grouped_indicator(self, df: DataFrame) -> DataFrame:
        _grouped_indicator_columns = [
            col.name for col in self.columns if col.is_indicator_group
        ]
        if len(_grouped_indicator_columns) > 0:
            get_dagster_logger().info(f"Add grouped indicator column")
            df['indicator_group'] = (
                df[_grouped_indicator_columns].astype(str).agg('---'.join, axis=1)
            )
        return df

    def _do_drop_columns_before_upload(self, df: DataFrame) -> DataFrame:
        for column in [col.name for col in self.columns if col.do_drop_before_upload]:
            get_dagster_logger().info(f"Drop column {column}")
            df.drop(column, axis=1, inplace=True)
            return df

    def postprocess(self, df: DataFrame, meta: dict) -> DataFrame:
        year_column = [col.name for col in self.columns if col.is_year][0]
        df = self._rename_columns(df)
        if self.custom_transform:
            df = self.custom_transform(df)
        df = self._decide_restructure_geo_value(df, 'geo_value_unstructured')
        df = self._restructure_year_column(df, year_column)
        df = self._add_source(df, meta)
        # self._add_column_grouped_indicator(df)  # disabled, needs a lot of memory
        df = self._do_drop_columns_before_upload(df)
        return df


def _custom_transform_quarter(df: DataFrame) -> DataFrame:
    get_dagster_logger().info(f"Apply custom transform for quarters")
    df = df[df['year'].str.endswith('Q4')]
    df['year'] = df['year'].str.replace(r'Q4$', '', regex=True)
    return df


CUBES = [
    StatTabCube(
        name='bev_demografisch_bilanz',
        bfs_id='px-x-0102020000_201',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32208094/master',
        columns=[
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('staatsangehorigkeit', is_indicator_group=True),
            ColumnDefinition('geschlecht', is_indicator_group=True),
            ColumnDefinition('indicator', is_indicator_group=True),
            ColumnDefinition('indicator_value'),
        ],
    ),
    StatTabCube(
        name='bev_zivilstand',
        bfs_id='px-x-0102010000_100',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32207867/master',
        columns=[
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('bevtyp', is_indicator_group=True),
            ColumnDefinition('geburtsort', is_indicator_group=True),
            ColumnDefinition('geschlecht', is_indicator_group=True),
            ColumnDefinition('zivilstand', is_indicator_group=True),
            ColumnDefinition('indicator_value'),
        ],
    ),
    StatTabCube(
        name='bev_heirat',
        bfs_id='px-x-0102020202_102',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32007787/master',
        columns=[
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('nationalitaet_a', is_indicator_group=True),
            ColumnDefinition('nationalitaet_b', is_indicator_group=True),
            ColumnDefinition('indicator_value'),
        ],
        encoding='latin1',
    ),
    StatTabCube(
        name='bev_scheidung',
        bfs_id='px-x-0102020203_103',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32007788/master',
        columns=[
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('ehedauer', is_indicator_group=True),
            ColumnDefinition('nationalitaet_a', is_indicator_group=True),
            ColumnDefinition('nationalitaet_b', is_indicator_group=True),
            ColumnDefinition('indicator_value'),
        ],
        encoding='latin1',
    ),
    StatTabCube(
        name='bev_altersklasse',
        bfs_id='px-x-0102010000_103',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32207863/master',
        columns=[
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('bevtyp', is_indicator_group=True),
            ColumnDefinition('geschlecht', is_indicator_group=True),
            ColumnDefinition('zivilstand', is_indicator_group=True),
            ColumnDefinition('altersklasse', is_indicator_group=True),
            ColumnDefinition('indicator_value'),
        ],
    ),
    StatTabCube(
        name='bev_geburtsort',
        bfs_id='px-x-0102010000_100',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32207867/master',
        columns=[
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('bevtyp', is_indicator_group=True),
            ColumnDefinition('geburtsort', is_indicator_group=True),
            ColumnDefinition('geschlecht', is_indicator_group=True),
            ColumnDefinition('zivilstand', is_indicator_group=True),
            ColumnDefinition('indicator_value'),
        ],
    ),
    StatTabCube(
        name='bev_zuwegzug',
        bfs_id='px-x-0103010200_121',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32208027/master',
        columns=[
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('nationalitaet', is_indicator_group=True),
            ColumnDefinition('geschlecht', is_indicator_group=True),
            ColumnDefinition('migrationstyp', is_indicator_group=True),
            ColumnDefinition('indicator_value'),
        ],
    ),
    StatTabCube(
        name='geb_bestand',
        bfs_id='px-x-0902020200_102',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32329355/master',
        columns=[
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('gebkategorie', is_indicator_group=True),
            ColumnDefinition('zimmer', is_indicator_group=True),
            ColumnDefinition('bauperiode', is_indicator_group=True),
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('indicator_value'),
        ],
    ),
    StatTabCube(
        name='geb_flaeche',
        bfs_id='px-x-0902020200_103',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32329364/master',
        columns=[
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('gebkategorie', is_indicator_group=True),
            ColumnDefinition('flaeche', is_indicator_group=True),
            ColumnDefinition('bauperiode', is_indicator_group=True),
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('indicator_value'),
        ],
    ),
    StatTabCube(
        name='geb_leerwhg',
        bfs_id='px-x-0902020300_101',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32386477/master',
        columns=[
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('wohnraum', is_indicator_group=True),
            ColumnDefinition('typ', is_indicator_group=True),
            ColumnDefinition('indikator', is_indicator_group=True),
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('indicator_value'),
        ],
    ),
    StatTabCube(
        name='bau_ausgaben',
        bfs_id='px-x-0904010000_201',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32130606/master',
        columns=[
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('auftraggeber', is_indicator_group=True),
            ColumnDefinition('bauwerk', is_indicator_group=True),
            ColumnDefinition('arbeit', is_indicator_group=True),
            ColumnDefinition('indikator', is_indicator_group=True),
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('indicator_value'),
        ],
        encoding='latin1',
    ),
    StatTabCube(
        name='raum_areal',
        bfs_id='px-x-0202020000_202',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32267662/master',
        columns=[
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('nolc04', is_indicator_group=True),
            ColumnDefinition('periode', is_year=True),
            ColumnDefinition('indicator_value'),
        ],
        encoding='latin1',
    ),
    StatTabCube(
        name='raum_noas',
        bfs_id='px-x-0202020000_102',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32267651/master',
        columns=[
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('noas04', is_indicator_group=True),
            ColumnDefinition('periode', is_year=True),
            ColumnDefinition('indicator_value'),
        ],
        encoding='latin1',
    ),
    StatTabCube(
        name='arbeit_grenzgaenger',
        bfs_id='px-x-0302010000_101',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/34087514/master',
        columns=[
            ColumnDefinition('geschlecht', is_indicator_group=True),
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('indicator_value'),
        ],
        encoding='latin1',
        custom_transform=_custom_transform_quarter,
    ),
    StatTabCube(
        name='wirtschaft_unternehmen',
        bfs_id='px-x-0602030000_205',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32846430/master',
        columns=[
            ColumnDefinition('indikator', is_indicator_group=True),
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('wirtschaftssektor', is_indicator_group=True),
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('indicator_value'),
        ],
        encoding='latin1',
    ),
    StatTabCube(
        name='wirtschaft_beschaeftigte',
        bfs_id='px-x-0602010000_102',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32159099/master',
        columns=[
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('wirtschaftssektor', is_indicator_group=True),
            ColumnDefinition('indikator', is_indicator_group=True),
            ColumnDefinition('indicator_value'),
        ],
        encoding='latin1',
    ),
    StatTabCube(
        name='lw_beschaeftigte',
        bfs_id='px-x-0702000000_104',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/31846452/master',
        columns=[
            ColumnDefinition('beobachtungseinheit', is_indicator_group=True),
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('betriebssystem', is_indicator_group=True),
            ColumnDefinition('year', is_year=True),
            ColumnDefinition('indicator_value'),
        ],
    ),
]


def stat_tab_factory(stat_tab_cube: StatTabCube) -> AssetsDefinition:

    @asset(
        compute_kind='python',
        group_name='src_bfs',
        key=['src', f'stat_tab_{stat_tab_cube.name}'],
        pool='stat_tab_extract',
        description=f"Extracts data from STAT-TAB ID {stat_tab_cube.bfs_id}",
    )
    def _asset(
        context: AssetExecutionContext,
        stat_tab: StatTabResource,
        db: PostgresResource,
    ) -> None:
        df, meta = stat_tab.load_data(stat_tab_cube.url, stat_tab_cube.encoding)
        df = stat_tab_cube.postprocess(df, meta)

        context.log.info(f"Writing data to database")
        df.to_sql(
            context.asset_key.path[-1],
            db.get_sqlalchemy_engine(),
            schema='src',
            if_exists='replace',
            index=False,
            chunksize=5_000_000,
        )

        # Insert metadata
        context.add_output_metadata(
            metadata={
                'num_records': len(df.index),
                'num_cols': len(df.columns),
                'preview': MetadataValue.md(df.head().to_markdown()),
            }
        )

    return _asset


assets_stat_tab = [stat_tab_factory(cube) for cube in CUBES]


@failure_hook(required_resource_keys={'pushover'})
def pushover_on_failure(context: HookContext):
    context.resources.pushover.send_failure_message(context)


@success_hook(required_resource_keys={'healthcheck'})
def ping_healthchecks(context: HookContext):
    """
    Pings healthchecks.io to notify that the pipeline is running.
    """
    context.resources.healthcheck.ping_by_env('HC__BFS_STAT_TAB')


job_bfs_stat_tab = define_asset_job(
    name='bfs_stat_tab',
    selection=[
        'seed_indicator*',
        *[f'src/stat_tab_{cube.name}*' for cube in CUBES],
    ],
    hooks={ping_healthchecks, pushover_on_failure},
)

schedule_stat_tab = ScheduleDefinition(
    job=job_bfs_stat_tab,
    cron_schedule='25 3 * * 6',  # once per week, on Saturday at 02:05
)
