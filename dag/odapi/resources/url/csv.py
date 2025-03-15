import io
import sys
from dataclasses import dataclass
from typing import List
from typing import Tuple

import pandas as pd
import requests
from dagster import ConfigurableResource


class UrlResource(ConfigurableResource):

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


class OpendataswissUrlResource(UrlResource):
    pass
