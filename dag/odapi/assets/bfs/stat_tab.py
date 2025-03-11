import re
from dataclasses import dataclass
from typing import List

import numpy as np
from dagster import AssetExecutionContext
from dagster import AssetsDefinition
from dagster import MetadataValue
from dagster import asset
from dagster import get_dagster_logger
from pandas import DataFrame

from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.url.stat_tab import StatTabResource


@dataclass
class ColumnDefinition:
    name: str
    is_indicator_group: bool = False
    do_drop_before_upload: bool = False


@dataclass
class StatTabCube:
    name: str
    bfs_id: str
    url: str
    columns: List[ColumnDefinition]

    def _rename_columns(self, df: DataFrame) -> DataFrame:
        get_dagster_logger().info(f"Renaming columns for cube {self.name}")
        df.columns = [col.name for col in self.columns]
        return df

    def _restructure_until_municipality_geo_value(
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
        df['geo_code'] = np.select(_conditions, _results, default=np.NaN)

        df['geo_value'] = df[unstructured_geo_column].str.extract(
            r'^\.\.\.\.\.\.(\d+)', expand=True
        )
        df['geo_value_name'] = df[unstructured_geo_column].str.extract(
            r'^(?:-|>>)(.*)', expand=True
        )
        return df

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
        self._rename_columns(df)
        self._restructure_until_municipality_geo_value(df, 'geo_value_unstructured')
        self._add_source(df, meta)
        # self._add_column_grouped_indicator(df)  # disabled, needs a lot of memory
        self._do_drop_columns_before_upload(df)
        return df


CUBES = [
    StatTabCube(
        name='bev_demografisch_bilanz',
        bfs_id='px-x-0102020000_201',
        url='https://dam-api.bfs.admin.ch/hub/api/dam/assets/32208094/master',
        columns=[
            ColumnDefinition('year'),
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
            ColumnDefinition('year'),
            ColumnDefinition('geo_value_unstructured', do_drop_before_upload=True),
            ColumnDefinition('bevtyp', is_indicator_group=True),
            ColumnDefinition('geburtsort', is_indicator_group=True),
            ColumnDefinition('geschlecht', is_indicator_group=True),
            ColumnDefinition('zivilstand', is_indicator_group=True),
            ColumnDefinition('indicator_value'),
        ],
    ),
]


def stat_tab_factory(stat_tab_cube: StatTabCube) -> AssetsDefinition:

    @asset(
        compute_kind='python',
        group_name='src_bfs',
        key=['src', f'stat_tab_{stat_tab_cube.name}'],
    )
    def _asset(
        context: AssetExecutionContext,
        stat_tab: StatTabResource,
        db: PostgresResource,
    ) -> None:
        df, meta = stat_tab.load_data(stat_tab_cube.url)
        df = stat_tab_cube.postprocess(df, meta)

        context.log.info(f"Writing data to database")
        df.to_sql(
            context.asset_key.path[-1],
            db.get_sqlalchemy_engine(),
            schema='src',
            if_exists='replace',
            index=False,
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

# XXX: Execute this once per week (and load new data)
