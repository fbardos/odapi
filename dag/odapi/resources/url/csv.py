import sys
import requests
import io
from dataclasses import dataclass
from dagster import ConfigurableResource
import pandas as pd
from typing import Tuple
from typing import List


@dataclass
class CkanResource:
    model_name: str
    ckan_resource_id: str


class UrlResource(ConfigurableResource):
    _URL_RESOURCES: List[CkanResource]

    # only needed to calculate size of raw data
    def _get_raw_csv(self, url: str) -> io.BytesIO:
        buffer = io.BytesIO()
        buffer.write(requests.get(url).content)
        buffer.seek(0)
        return buffer

    def load_data(self, url: str) -> Tuple[pd.DataFrame, int]:
        """Returns data as pandas.DataFrame and size in bytes."""
        _data_buffer = self._get_raw_csv(url)
        size_rawdata_bytes = sys.getsizeof(_data_buffer.getvalue())
        return pd.read_csv(_data_buffer), size_rawdata_bytes


class KtzhGemeportraitUrlResource(UrlResource):
    _URL_RESOURCES: List[CkanResource] = [
        CkanResource(model_name='ktzh_gp_bevoelkerung',             ckan_resource_id='132b6fed-d7ea-48e3-b5dc-9e63ac16b21e'),
        CkanResource(model_name='ktzh_gp_auslaenderanteil',         ckan_resource_id='23cc674b-2eb6-4ad5-9ddf-87e86f0fb06f'),
        CkanResource(model_name='ktzh_gp_avg_haushaltsgroesse',     ckan_resource_id='ae3cc772-38e7-4d5f-87f2-73ad8e5d07c1'),
    ]

