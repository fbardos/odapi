"""
Data from Schweizer Städteverband, Statistik der Schweizer Städte
Remarks: Statistik der Schweizer Städte 2021 contains data from 2020.

Source:
    - https://staedteverband.ch/de/Info/publikationen/statistik-der-schweizer-stadte

"""

from dataclasses import dataclass
from typing import Any
from typing import List
from typing import Optional
from typing import Sequence
from typing import Type

import pandas as pd
from dagster import AssetExecutionContext
from dagster import AssetsDefinition
from dagster import TimeWindowPartitionsDefinition
from dagster import asset
from dagster import define_asset_job

from odapi.resources.postgres.postgres import PostgresResource
from odapi.resources.url.excel import SssExcelResource


@dataclass
class SchweizerStaedteSourceConfig:
    source_url: str
    year_offset: int


@dataclass
class ColumnConfig:
    name: str
    include_years: Optional[List[int]] = None
    exclude_years: Optional[List[int]] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None

    def is_included_in_year(self, year: int) -> bool:
        if self.include_years:
            return year in self.include_years
        if self.exclude_years:
            return year not in self.exclude_years
        if self.start_year and self.end_year:
            return self.start_year <= year <= self.end_year
        if self.start_year:
            return year >= self.start_year
        if self.end_year:
            return year <= self.end_year
        return True


@dataclass
class SchweizerStaedteSheetConfig:
    sheet_name: str
    skiprows: int
    source: str
    columns: List[ColumnConfig]
    usecols: Optional[str | Sequence[int]] = None

    # XXX: Maybe not needed anymore with columns as List of ColumnConfig
    def columns_to_year_mapping(self, partition_year: str) -> List[str]:
        """Starting with 2023, the first column is gemeinde_bfs_id."""
        if int(partition_year) >= 2023:
            return ['gemeinde_bfs_id'] + self.columns
        else:
            return self.columns

    # XXX: Maybe not needed anymore with columns as List of ColumnConfig
    def usecols_to_year_mapping(self, usecols: Any, partition_year: str) -> Any:
        """Starting with 2023, the first column is gemeinde_bfs_id. So when using a range, must be adjusted too."""
        if isinstance(usecols, range):
            if int(partition_year) >= 2023:
                return range(len(usecols) + 1)
            else:
                return usecols
        else:
            return usecols

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Can be overwritten by subclass if needed."""
        return data


class SssFileconfig:
    FILE_CATEGORY: str
    XLSX_URLS: dict[str, SchweizerStaedteSourceConfig]
    SHEET_CONFIGS: dict[str, SchweizerStaedteSheetConfig]


class ConfigBevoelkerung(SssFileconfig):
    FILE_CATEGORY = 'bevoelkerung'
    XLSX_URLS = {
        '2020': SchweizerStaedteSourceConfig(
            'https://staedteverband.ch/cmsfiles/t_stst_2020_01_bev%C3%B6lkerung.xlsx?v=20250118095059',
            year_offset=2,
        ),
        '2021': SchweizerStaedteSourceConfig(
            'https://staedteverband.ch/cmsfiles/t_stst_2021_01_bev%C3%B6lkerung.xlsx?v=20250118162758',
            year_offset=2,
        ),
        '2022': SchweizerStaedteSourceConfig(
            'https://staedteverband.ch/cmsfiles/t_stst_2022_01_bev%C3%B6lkerung.xlsx?v=20250118162815',
            year_offset=2,
        ),
        '2023': SchweizerStaedteSourceConfig(
            'https://staedteverband.ch/cmsfiles/t_01_stst_2023_bev%C3%B6lkerung_2.xlsx?v=20250118162830',
            year_offset=2,
        ),
        '2024': SchweizerStaedteSourceConfig(
            'https://staedteverband.ch/cmsfiles/t_01_stst_2024_bev%C3%B6lkerung_2.xlsx?v=20250118162846',
            year_offset=2,
        ),
    }
    SHEET_CONFIGS = {
        'wohnbevoelkerung': SchweizerStaedteSheetConfig(
            sheet_name='T 1.1',
            skiprows=4,
            columns=[
                ColumnConfig('gemeinde_bfs_id', start_year=2023),
                ColumnConfig('gemeinde_name'),
                ColumnConfig('staendig_total_yyyy'),
                ColumnConfig('staendig_total_vor_yyyy_minus_10'),
                ColumnConfig('staendig_veraenderung_10_jahre'),
                ColumnConfig('staendig_bevoelkerungsdichte_yyyy'),
                ColumnConfig('volkszaehlung_2000'),
                ColumnConfig('volkszaehlung_1990'),
                ColumnConfig('volkszaehlung_1980'),
                ColumnConfig('volkszaehlung_1970'),
                ColumnConfig('volkszaehlung_1930'),
            ],
            source='BFS – Arealstatistik der Schweiz (AREA), Statistik der Bevölkerung und der Haushalte (STATPOP), Statistik des jährlichen Bevölkerungsstandes (ESPOP), Eidgenössische Volkszählung (VZ)',
        ),
        'geschlecht_zivilstand': SchweizerStaedteSheetConfig(
            sheet_name='T 1.2a',
            skiprows=4,
            columns=[
                ColumnConfig('gemeinde_bfs_id', start_year=2023),
                ColumnConfig('gemeinde_name'),
                ColumnConfig('frauen'),
                ColumnConfig('maenner'),
                ColumnConfig('ledig'),
                ColumnConfig('verheiratet_eingetragen_partnerschaft'),
                ColumnConfig('verwitwet'),
                ColumnConfig('geschieden'),
                # heiraten and scheidungen are not available for all years
                # 'heiraten',
                # 'scheidungen',
            ],
            source='BFS – Statistik der Bevölkerung und der Haushalte (STATPOP), Statistik der natürlichen Bevölkerungsbewegung (BEVNAT)',
            usecols=range(7),  # for 2023+ will be replaced with range(8)
        ),
        'altersklassen': SchweizerStaedteSheetConfig(
            sheet_name='T 1.2b',
            skiprows=4,
            columns=[
                ColumnConfig('gemeinde_bfs_id', start_year=2023),
                ColumnConfig('gemeinde_name'),
                ColumnConfig('total'),
                ColumnConfig('age0_4'),
                ColumnConfig('age5_9'),
                ColumnConfig('age10_14'),
                ColumnConfig('age15_19'),
                ColumnConfig('age20_24'),
                ColumnConfig('age25_29'),
                ColumnConfig('age30_34'),
                ColumnConfig('age35_39'),
                ColumnConfig('age40_44'),
                ColumnConfig('age45_49'),
                ColumnConfig('age50_54'),
                ColumnConfig('age55_59'),
                ColumnConfig('age60_64'),
                ColumnConfig('age65_69'),
                ColumnConfig('age70_74'),
                ColumnConfig('age75_79'),
                ColumnConfig('age80_84'),
                ColumnConfig('age85_89'),
                ColumnConfig('age90_94'),
                ColumnConfig('age95_99'),
                ColumnConfig('age100_plus'),
            ],
            source='BFS – Statistik der Bevölkerung und der Haushalte (STATPOP)',
        ),
        'bilanz': SchweizerStaedteSheetConfig(
            sheet_name='T 1.3',
            skiprows=4,
            columns=[
                ColumnConfig('gemeinde_bfs_id', start_year=2023),
                ColumnConfig('gemeinde_name'),
                ColumnConfig('wohnbevoelkerung_jahresanfang'),
                ColumnConfig('lebendgeburten'),
                ColumnConfig('todesfaelle'),
                ColumnConfig('geburtenueberschuss'),
                ColumnConfig('zuzug'),
                ColumnConfig('wegzug'),
                ColumnConfig('wanderungssaldo'),
                ColumnConfig('bev_gesamtbilanz'),
                ColumnConfig('bev_gesamtbilanz_prozent'),
                ColumnConfig('wohnbevoelkerung_jahresende'),
                ColumnConfig('lebendgeburten_pro_1000'),
                ColumnConfig('todesfaelle_pro_1000'),
                ColumnConfig('geburtenueberschuss_pro_1000'),
                ColumnConfig('zuzug_pro_1000'),
                ColumnConfig('wegzug_pro_1000'),
                ColumnConfig('wanderungssaldo_pro_1000'),
                ColumnConfig('bev_gesamtbilanz_pro_1000'),
            ],
            source='Statistik der Bevölkerung und der Haushalte (STATPOP), Statistik der natürlichen Bevölkerungsbewegung (BEVNAT)',
        ),
        'aufenthalt': SchweizerStaedteSheetConfig(
            sheet_name='T 1.5a',
            skiprows=4,
            columns=[
                ColumnConfig('gemeinde_bfs_id', start_year=2023),
                ColumnConfig('gemeinde_name'),
                ColumnConfig('auslaender'),
                ColumnConfig('aufenthalt_b'),
                ColumnConfig('niedergelassen_c'),
                ColumnConfig('vorl_aufgenommen_f'),
                ColumnConfig('kurzaufenthalter_l'),
                ColumnConfig('asylsuchend_n'),
                ColumnConfig('schutzstatus_s', start_year=2024),
                ColumnConfig('diplomat'),
                ColumnConfig('andere'),
            ],
            source='BFS - Statistik der Bevölkerung und der Haushalte (STATPOP)',
        ),
        'herkunft': SchweizerStaedteSheetConfig(
            sheet_name='T 1.5b',
            skiprows=4,
            columns=[
                ColumnConfig('gemeinde_bfs_id', start_year=2023),
                ColumnConfig('gemeinde_name'),
                ColumnConfig('auslaender'),
                ColumnConfig('auslaender_anteil'),
                ColumnConfig('eu_efta_total'),
                ColumnConfig('deutschland'),
                ColumnConfig('frankreich'),
                ColumnConfig('italien'),
                ColumnConfig('oesterreich'),
                ColumnConfig('spanien'),
                ColumnConfig('portugal'),
                ColumnConfig('uebrige_eu_total'),
                ColumnConfig('serbien'),
                ColumnConfig('tuerkei'),
                ColumnConfig('nordmazedonien'),
                ColumnConfig('russland'),
                ColumnConfig('asien_total'),
                ColumnConfig('sri_lanka'),
                ColumnConfig('indien'),
                ColumnConfig('china'),
                ColumnConfig('afrika'),
                ColumnConfig('nord_sued_amerika'),
                ColumnConfig('australasien'),
                ColumnConfig('andere_laender'),
            ],
            source='BFS - Statistik der Bevölkerung und der Haushalte (STATPOP)',
        ),
    }


YEARLY_PARTITIONS_DEF = TimeWindowPartitionsDefinition(
    cron_schedule="0 5 5 5 *",
    fmt="%Y",
    start="2020",  # links for 2019 are currently not available
    end_offset=1,
)


def sss_statistk_schweizer_staedte_factory(
    config: Type[SssFileconfig], sheet_key: str
) -> AssetsDefinition:
    """Creates one asset per sheet.

    One asset per file is not enough, because they get persisted in different tables.

    """
    sheet = config.SHEET_CONFIGS[sheet_key]
    assert isinstance(sheet, SchweizerStaedteSheetConfig)

    @asset(
        compute_kind='python',
        group_name='src_bfs',
        key=['src', f'sss_{config.FILE_CATEGORY}_{sheet_key}'],
        partitions_def=YEARLY_PARTITIONS_DEF,
    )
    def sss_asset(
        context: AssetExecutionContext,
        excel_sss: SssExcelResource,
        db: PostgresResource,
    ):
        source_config = ConfigBevoelkerung.XLSX_URLS[context.partition_key]
        _active_cols = [
            col.name
            for col in sheet.columns
            if col.is_included_in_year(int(context.partition_key))
        ]
        context.log.debug(f'Active columns in {context.partition_key}: {_active_cols}')
        _inactive_cols = [
            col.name
            for col in sheet.columns
            if not col.is_included_in_year(int(context.partition_key))
        ]
        context.log.debug(
            f'Inactive columns in {context.partition_key}: {_inactive_cols}'
        )
        _usecols = range(len(_active_cols))
        df = excel_sss.load_excel_by_url(
            url=source_config.source_url,
            sheet_name=sheet.sheet_name,
            skiprows=sheet.skiprows,
            names=_active_cols,
            index_col=None,
            usecols=_usecols,
        )
        df = sheet.transform(df)
        context.log.info('Executing custom post processing.')

        # removing empty rows must be executed before inserting empty columns
        df = excel_sss.remove_empty_rows(df)
        df = excel_sss.add_empty_inactive_columns(df, columns=_inactive_cols)
        df = df[[col.name for col in sheet.columns]]  # reorder columns
        assert isinstance(df, pd.DataFrame)
        if sheet.source:
            df = excel_sss.add_source_column(df, sheet.source)
        df = excel_sss.add_year_column(
            df, context.partition_key, year_offset=source_config.year_offset
        )

        # Write to database
        context.log.info('Loading DataFrame to database.')
        df.to_sql(
            context.asset_key.path[-1],
            db.get_sqlalchemy_engine(),
            schema='src',
            if_exists='replace',
            index=False,
        )

    return sss_asset


# definitions
assets = []
asset_keys = []
for file_config in [
    ConfigBevoelkerung,
]:
    assert issubclass(file_config, SssFileconfig)
    for sheet_key in file_config.SHEET_CONFIGS.keys():
        assets.append(sss_statistk_schweizer_staedte_factory(file_config, sheet_key))
        asset_keys.append(f'sss_{file_config.FILE_CATEGORY}_{sheet_key}')

job_sss = define_asset_job(
    name='job_sss',
    selection=['seed_indicator*', *[f'src/{asset_key}*' for asset_key in asset_keys]],
    partitions_def=YEARLY_PARTITIONS_DEF,
)
