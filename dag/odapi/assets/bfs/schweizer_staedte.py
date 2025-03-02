"""
Data from Schweizer Städteverband, Statistik der Schweizer Städte
Remarks: Statistik der Schweizer Städte 2021 contains data from 2020.

Source:
    - https://staedteverband.ch/de/Info/publikationen/statistik-der-schweizer-stadte

"""
from dataclasses import dataclass
from dagster import asset
from dagster import AssetExecutionContext
from odapi.resources.url.excel import SssExcelResource
from typing import List
from dagster import TimeWindowPartitionsDefinition
from odapi.resources.postgres.postgres import PostgresResource


@dataclass
class SchweizerStaedteSourceConfig:
    source_url: str
    year_offset: int

@dataclass
class SchweizerStaedteSheetConfig:
    sheet_name: str
    skiprows: int
    source: str
    columns: List[str]

    def columns_to_year_mapping(self, partition_year: str) -> List[str]:
        """Starting with 2023, the first column is gemeinde_bfs_id."""
        if int(partition_year) >= 2023:
            return ['gemeinde_bfs_id'] + self.columns
        else:
            return self.columns

class ConfigBevoelkerung:
    XLSX_URLS = {
        '2020': SchweizerStaedteSourceConfig('https://staedteverband.ch/cmsfiles/t_stst_2020_01_bev%C3%B6lkerung.xlsx?v=20250118095059', 2),
        '2021': SchweizerStaedteSourceConfig('https://staedteverband.ch/cmsfiles/t_stst_2021_01_bev%C3%B6lkerung.xlsx?v=20250118162758', 2),
        '2022': SchweizerStaedteSourceConfig('https://staedteverband.ch/cmsfiles/t_stst_2022_01_bev%C3%B6lkerung.xlsx?v=20250118162815', 2),
        '2023': SchweizerStaedteSourceConfig('https://staedteverband.ch/cmsfiles/t_01_stst_2023_bev%C3%B6lkerung_2.xlsx?v=20250118162830', 2),
        '2024': SchweizerStaedteSourceConfig('https://staedteverband.ch/cmsfiles/t_01_stst_2024_bev%C3%B6lkerung_2.xlsx?v=20250118162846', 2),
    }
    WOHNBEVOELKERUNG = SchweizerStaedteSheetConfig(
        sheet_name='T 1.1',
        skiprows=4,
        columns=[
            'gemeinde_name',
            'staendig_total_yyyy',
            'staendig_total_vor_yyyy_minus_10',
            'staendig_veraenderung_10_jahre',
            'staendig_bevoelkerungsdichte_yyyy',
            'volkszaehlung_2000',
            'volkszaehlung_1990',
            'volkszaehlung_1980',
            'volkszaehlung_1970',
            'volkszaehlung_1930',
        ],
        source='BFS – Arealstatistik der Schweiz (AREA), Statistik der Bevölkerung und der Haushalte (STATPOP), Statistik des jährlichen Bevölkerungsstandes (ESPOP), Eidgenössische Volkszählung (VZ)',
    )
    GESCHLECHT_ZIVILSTAND = SchweizerStaedteSheetConfig(
        sheet_name='T 1.2a',
        skiprows=4,
        columns=[
            'gemeinde_name',
            'frauen',
            'maenner',
            'ledig',
            'verheiratet_eingetragen_partnerschaft',
            'verwitwet',
            'geschieden',
            # heiraten and scheidungen are not available for all years
            # 'heiraten',
            # 'scheidungen',
        ],
        source='BFS – Statistik der Bevölkerung und der Haushalte (STATPOP), Statistik der natürlichen Bevölkerungsbewegung (BEVNAT)',
    )

# CONFIG_BEVOELKERUNG = {
    # 'WOHNBEVO'

# }

YEARLY_PARTITIONS_DEF = TimeWindowPartitionsDefinition(
    cron_schedule="0 5 5 5 *",
    fmt="%Y",
    start="2019",
    end_offset=1,
)


@asset(
    compute_kind='python',
    group_name='src_bfs',
    key=['src', 'sss_bevoelkerung_wohnbevoelkerung'],
    partitions_def=YEARLY_PARTITIONS_DEF,
)
def sss_bevoelkerung_wohnbevoelkerung(
    context: AssetExecutionContext,
    excel_sss: SssExcelResource,
    db: PostgresResource,
):
    config = ConfigBevoelkerung.WOHNBEVOELKERUNG
    source_config = ConfigBevoelkerung.XLSX_URLS[context.partition_key]
    df = excel_sss.load_excel_by_url(
        url=source_config.source_url,
        source=config.source,
        sheet_name=config.sheet_name,
        skiprows=config.skiprows,
        names=config.columns_to_year_mapping(context.partition_key),
        index_col=None,
    )

    context.log.info('Executing custom post processing.')
    df = excel_sss.remove_empty_rows(df)
    df = excel_sss.add_year_column(df, context.partition_key, year_offset=source_config.year_offset)

    # Write to database
    context.log.info('Loading DataFrame to database.')
    df.to_sql(context.asset_key.path[-1], db.get_sqlalchemy_engine(), schema='src', if_exists='replace', index=False)

@asset(
    compute_kind='python',
    group_name='src_bfs',
    key=['src', 'sss_bevoelkerung_geschlecht_zivilstand'],
    partitions_def=YEARLY_PARTITIONS_DEF,
)
def sss_bevoelkerung_geschlecht_zivilstand(
    context: AssetExecutionContext,
    excel_sss: SssExcelResource,
    db: PostgresResource,
):
    config = ConfigBevoelkerung.GESCHLECHT_ZIVILSTAND
    source_config = ConfigBevoelkerung.XLSX_URLS[context.partition_key]
    df = excel_sss.load_excel_by_url(
        url=source_config.source_url,
        source=config.source,
        sheet_name=config.sheet_name,
        skiprows=config.skiprows,
        names=config.columns_to_year_mapping(context.partition_key),
        engine='openpyxl',
        usecols=range(7),  # usecols='A:I' does not work properly, use index instead
    )
    df = excel_sss.remove_empty_rows(df)
    df = excel_sss.add_year_column(df, context.partition_key, year_offset=source_config.year_offset)

    # Write to database
    context.log.info('Loading DataFrame to database.')
    df.to_sql(context.asset_key.path[-1], db.get_sqlalchemy_engine(), schema='src', if_exists='replace', index=False)
