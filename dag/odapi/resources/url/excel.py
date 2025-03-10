import io
import sys
from dataclasses import dataclass
from typing import List
from typing import Optional
from typing import Tuple

import numpy as np
import pandas as pd
import requests
from dagster import ConfigurableResource
from dagster import get_dagster_logger


class ExcelResource(ConfigurableResource):

    def postprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """Empty by default."""
        return df

    def load_excel_by_url(self, url: str, **kwargs) -> pd.DataFrame:
        """Reads Excel file from URL and returns it as pandas.DataFrame.

        Args:
            url (str): URL to Excel file.
            **kwargs: Keyword arguments to pass to `pd.read_excel`.
        """
        logger = get_dagster_logger()
        logger.info(f'Reading Excel file from URL: {url}')
        df = pd.read_excel(url, **kwargs)
        logger.info(f'Postprocessing loaded DataFrame')
        df = self.postprocess(df)
        return df


class SssExcelResource(ExcelResource):
    """Special handling for tables from `Statistik der Schweizer Städte`."""

    def postprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """Postprocessing for tables from `Statistik der Schweizer Städte`."""

        # Some municipality names have an asterisk at the end
        # to mark some notes. Remove them.
        df['gemeinde_name'] = df['gemeinde_name'].str.replace(r'\*$', '', regex=True)

        return df

    def remove_empty_rows(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return df.dropna(**kwargs)

    def add_year_column(
        self, df: pd.DataFrame, partition_year: str, year_offset: int = 1
    ) -> pd.DataFrame:
        # Statistik der Schweizer Städte 2023 contains data from the end of year 2022.
        df['year'] = int(partition_year) - year_offset
        return df

    def add_source_column(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        df['source'] = source
        return df

    def add_empty_inactive_columns(
        self, df: pd.DataFrame, columns: List[str]
    ) -> pd.DataFrame:
        """Add empty columns for columns that are not present in the provided partition_year.

        This is needed to ensure that the DataFrame has the same columns for all years (for DB import).

        """
        for col in columns:
            df[col] = np.nan
        return df

    # # only needed to calculate size of raw data
    # def _get_raw_csv(self, url: str) -> io.BytesIO:
    # buffer = io.BytesIO()
    # buffer.write(requests.get(url).content)
    # buffer.seek(0)
    # return buffer

    # def load_data(self, url: str) -> Tuple[pd.DataFrame, int]:
    # """Returns data as pandas.DataFrame and size in bytes."""
    # _data_buffer = self._get_raw_csv(url)
    # size_rawdata_bytes = sys.getsizeof(_data_buffer.getvalue())
    # return pd.read_csv(_data_buffer), size_rawdata_bytes


# # class KtzhGemeportraitUrlResource(UrlResource):
# # _URL_RESOURCES: List[CkanResource] = [
# # ]

# class OpendataswissUrlResource(UrlResource):
# _URL_RESOURCES: List[CkanResource] = [
# CkanResource(model_name='bfe_minergie',                     ckan_resource_id='3ae6d523-748c-466b-8368-04569473338e'),
# CkanResource(model_name='ktzh_gp_bevoelkerung',             ckan_resource_id='132b6fed-d7ea-48e3-b5dc-9e63ac16b21e'),
# CkanResource(model_name='ktzh_gp_auslaenderanteil',         ckan_resource_id='23cc674b-2eb6-4ad5-9ddf-87e86f0fb06f'),
# CkanResource(model_name='ktzh_gp_avg_haushaltsgroesse',     ckan_resource_id='ae3cc772-38e7-4d5f-87f2-73ad8e5d07c1'),
# ]
