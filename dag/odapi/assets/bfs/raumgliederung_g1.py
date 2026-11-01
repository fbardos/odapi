import datetime as dt
import io
from dataclasses import dataclass
from typing import Literal

import dagster as dg
import dlt
import geopandas as gpd
import pandas as pd
import pyarrow as pa
import requests
from dagster import AssetExecutionContext
from dagster import TimeWindowPartitionsDefinition
from dagster import build_schedule_from_partitioned_job


@dataclass
class SimplifiedBordersConfig:
    api_level: Literal['Commune', 'District', 'Canton', 'MSREG1980']
    resolution: Literal['G1', 'K4']

    _API_BASE_URL: str = 'https://www.agvchapp.bfs.admin.ch/api/boundaries/divisions'

    def make_request(self, duedate: dt.date) -> requests.Response:
        return requests.get(
            self._API_BASE_URL,
            params=dict(
                division=self.api_level,
                resolution=self.resolution,
                dissolve=True,
                date=duedate.strftime('%d-%m-%Y'),
            ),
        )


@dlt.resource(
    write_disposition='replace',
)
def _resource(
    context: AssetExecutionContext,
    config: SimplifiedBordersConfig,
) -> pa.Table:
    date = dt.date.fromisoformat(context.partition_key)
    response = config.make_request(date)
    response.raise_for_status()
    gdf = gpd.read_file(io.BytesIO(response.content))

    # transformation, for clickhouse
    if gdf.crs is None:
        gdf = gdf.set_crs('EPSG:2056')
    gdf = gdf.to_crs('EPSG:4326')
    gdf['stichtag'] = date
    # Workaround, because date would be parsed by dlt as Date32,
    # which is outside range for some Dagster partitions
    gdf['stichtag'] = pd.to_datetime(gdf['stichtag'], utc=True)
    geo_col = gdf.geometry.name
    table = pa.Table.from_pandas(
        gdf.drop(columns=geo_col),
        preserve_index=False,
    )

    yield table.append_column(
        geo_col,
        pa.array(
            gdf.geometry.to_wkb(),
            type=pa.binary(),
        ),
    )


@dlt.source
def dlt_source(
    context: AssetExecutionContext,
):
    return (
        _resource(
            context=context,
            config=SimplifiedBordersConfig(
                api_level='Commune',
                resolution='G1',  # middle precision
            ),
        ).with_name('bfs_gemeinde_g1'),
        _resource(
            context=context,
            config=SimplifiedBordersConfig(
                api_level='District',
                resolution='G1',  # middle precision
            ),
        ).with_name('bfs_bezirk_g1'),
        _resource(
            context=context,
            config=SimplifiedBordersConfig(
                api_level='Canton',
                resolution='G1',  # middle precision
            ),
        ).with_name('bfs_kanton_g1'),
    )


dlt_pipeline = dlt.pipeline(
    pipeline_name='bfs_raumgliederung_generalisiert',
    destination='clickhouse',
    # no destination for Clickhouse, because no schema exists
)

PARTITION_DEF = TimeWindowPartitionsDefinition(
    cron_schedule="0 0 1 1 *",
    fmt='%Y-%m-%d',
    start='1850-01-01',
    end_offset=1,
)


@dg.asset(
    name=f'dlt_bfs_raumgliederung_generalisiert',
    partitions_def=PARTITION_DEF,
    kinds={'dlt', 'clickhouse'},
)
def _asset(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    load_info = dlt_pipeline.run(dlt_source(context=context))
    return dg.MaterializeResult(metadata={'dlt_load_info': str(load_info)})


job = dg.define_asset_job(
    name='bfs_borders_simplified',
    selection=dg.AssetSelection.assets(
        _asset,
    ).downstream(),
)

schedule = build_schedule_from_partitioned_job(
    job,
)
