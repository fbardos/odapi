"""
Data from Schweizer Städteverband, Statistik der Schweizer Städte
Remarks: Statistik der Schweizer Städte 2021 contains data from 2020.

Source:
    - https://staedteverband.ch/de/Info/publikationen/statistik-der-schweizer-stadte

"""

from dataclasses import dataclass
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
class SchweizerStaedteSheetConfig:
    sheet_name: str
    skiprows: int
    source: str
    columns: List[str]
    usecols: Optional[str | Sequence[int]] = None

    def columns_to_year_mapping(self, partition_year: str) -> List[str]:
        """Starting with 2023, the first column is gemeinde_bfs_id."""
        if int(partition_year) >= 2023:
            return ['gemeinde_bfs_id'] + self.columns
        else:
            return self.columns

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
        ),
        'geschlecht_zivilstand': SchweizerStaedteSheetConfig(
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
            usecols=range(7),
        ),
        'altersklassen': SchweizerStaedteSheetConfig(
            sheet_name='T 1.2b',
            skiprows=4,
            columns=[
                'gemeinde_name',
                'total',
                'age0_4',
                'age5_9',
                'age10_14',
                'age15_19',
                'age20_24',
                'age25_29',
                'age30_34',
                'age35_39',
                'age40_44',
                'age45_49',
                'age50_54',
                'age55_59',
                'age60_64',
                'age65_69',
                'age70_74',
                'age75_79',
                'age80_84',
                'age85_89',
                'age90_94',
                'age95_99',
                'age100_plus',
            ],
            source='BFS – Statistik der Bevölkerung und der Haushalte (STATPOP)',
        ),
        'bilanz': SchweizerStaedteSheetConfig(
            sheet_name='T 1.3',
            skiprows=4,
            columns=[
                'gemeinde_name',
                'wohnbevoelkerung_jahresanfang',
                'lebendgeburten',
                'todesfaelle',
                'geburtenueberschuss',
                'zuzug',
                'wegzug',
                'wanderungssaldo',
                'bev_gesamtbilanz',
                'bev_gesamtbilanz_prozent',
                'wohnbevoelkerung_jahresende',
                'lebendgeburten_pro_1000',
                'todesfaelle_pro_1000',
                'geburtenueberschuss_pro_1000',
                'zuzug_pro_1000',
                'wegzug_pro_1000',
                'wanderungssaldo_pro_1000',
                'bev_gesamtbilanz_pro_1000',
            ],
            source='Statistik der Bevölkerung und der Haushalte (STATPOP), Statistik der natürlichen Bevölkerungsbewegung (BEVNAT)',
        ),
        'aufenthalt': SchweizerStaedteSheetConfig(
            sheet_name='T 1.5a',
            skiprows=4,
            columns=[
                'gemeinde_name',
                'auslaender',
                'aufenthalt_b',
                'niedergelassen_c',
                'vorl_aufgenommen_f',
                'kurzaufenthalter_l',
                'asylsuchend_n',
                'diplomat',
                'andere',
            ],
            source='BFS - Statistik der Bevölkerung und der Haushalte (STATPOP)',
        ),
        'herkunft': SchweizerStaedteSheetConfig(
            sheet_name='T 1.5b',
            skiprows=4,
            columns=[
                'gemeinde_name',
                'auslaender',
                'auslaender_anteil',
                'eu_efta_total',
                'deutschland',
                'frankreich',
                'italien',
                'oesterreich',
                'spanien',
                'portugal',
                'uebrige_eu_total',
                'serbien',
                'tuerkei',
                'nordmazedonien',
                'russland',
                'asien_total',
                'sri_lanka',
                'indien',
                'china',
                'afrika',
                'nord_sued_amerika',
                'australasien',
                'andere_laender',
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
        df = excel_sss.load_excel_by_url(
            url=source_config.source_url,
            source=sheet.source,
            sheet_name=sheet.sheet_name,
            skiprows=sheet.skiprows,
            names=sheet.columns_to_year_mapping(context.partition_key),
            index_col=None,
            usecols=sheet.usecols,
        )
        df = sheet.transform(df)

        context.log.info('Executing custom post processing.')
        df = excel_sss.remove_empty_rows(df)
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
    selection=[f'src/{asset_key}*' for asset_key in asset_keys],
    partitions_def=YEARLY_PARTITIONS_DEF,
)
