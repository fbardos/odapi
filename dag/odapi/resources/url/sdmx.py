"""

BFS documentation:
 - https://www.bfs.admin.ch/bfs/en/home/services/research/swiss-stats-explorer.html

"""

import datetime as dt
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from io import BytesIO
from typing import List
from typing import Optional

import numpy as np
import pandas as pd
import requests
import sdmx
from dagster import ConfigurableResource
from dagster import get_dagster_logger
from sdmx.client import Client
from sdmx.model.common import BaseDataflow
from sdmx.model.common import BaseDataStructureDefinition
from sdmx.model.common import Component
from sdmx.model.common import DataAttribute
from sdmx.model.common import DimensionComponent
from sdmx.model.common import ItemScheme
from sdmx.model.common import Representation
from sdmx.source import Source

from odapi.debug_tty import set_trace
from odapi.resources.url.requests_info import RequestsInfo

DEFAULT_SUPPORTED = {
    "organisationscheme": False,
    "dataproviderscheme": False,
    "dataconsumerscheme": False,
    "provisionagreement": False,
    "structure": False,
    "structureset": False,
}


@dataclass
class ProcessedDimensions:
    data: pd.DataFrame
    dimensions: List[str]


@dataclass
class SdmxConfig:
    name: str
    agency_id: str
    dataflow_id: str
    dataflow_version: str
    url: str = 'https://disseminate.stats.swiss/rest'
    supports: dict = field(default_factory=lambda: DEFAULT_SUPPORTED)
    source_overwrite: Optional[str] = None

    @property
    def dir_name(self) -> str:
        return '__'.join([self.agency_id, self.dataflow_id])

    @property
    def sdmx_source_info(self) -> dict:
        return dict(
            id=self.agency_id,
            url=self.url,
            name=self.name,
            supports=self.supports,
        )

    def add_source(self) -> Source:
        sdmx.add_source(info=self.sdmx_source_info, override=True)
        return sdmx.get_source(self.agency_id)

    @property
    def client(self) -> Client:
        self.add_source()
        return sdmx.Client(self.agency_id)

    @property
    def dataflow(self) -> BaseDataflow:
        exr_msg = self.client.dataflow(self.dataflow_id)
        return exr_msg.dataflow.get(self.dataflow_id)

    @property
    def dsd(self) -> BaseDataStructureDefinition:
        return self.dataflow.structure

    @property
    def attributes(self) -> List[DataAttribute]:
        return self.dsd.attributes.components

    @property
    def dimensions(self) -> List[DimensionComponent]:
        return self.dsd.dimensions.components

    def codelist_by_id(self, id: str) -> ItemScheme | None:
        logger = get_dagster_logger()
        dimension = self.dsd.dimensions.get(id)
        if dimension is None:
            logger.warning(f'No dimension information for DIM {id} found.')
            return None
        else:
            assert isinstance(dimension, Component)
            assert isinstance(dimension.local_representation, Representation)
            enumerated = dimension.local_representation.enumerated
            if enumerated is None:
                logger.warning(f'No codelist for DIM {id} found.')
                return None
            else:
                assert isinstance(enumerated, ItemScheme)
                logger.debug(f'Returning codelist for DIM {id}.')
                return enumerated

    def codelist_series_by_id(self, id: str) -> pd.Series | None:
        codelist = self.codelist_by_id(id)
        if codelist is None:
            return None
        else:
            return sdmx.to_pandas(codelist)

    def dataflow_url(self, base_url: str) -> str:
        return "/".join(
            [
                base_url,
                'dataflow',
                self.agency_id,
                self.dataflow_id,
                self.dataflow_version,
            ]
        )

    def data_url(self, base_url: str) -> str:
        return "/".join(
            [
                base_url,
                'data',
                self.agency_id + ',' + self.dataflow_id + ',' + self.dataflow_version,
                'all',
            ]
        )

    def dim_suffix(self, dim: str) -> str:
        return f'_{dim}'

    def dim_dict(self, dim: str) -> str:
        return f'dict_{dim}'

    def expand_dimensions(
        self, data: pd.DataFrame, skip_dims: List[str] = []
    ) -> ProcessedDimensions:
        logger = get_dagster_logger()
        processed_dimensions = []
        # The following pandas.merge will apply the suffix only, if
        # the left and right table have the same column names. But
        # we want to apply the suffix every time, also for the first
        # dimension. For this, add empty columns on the left table.
        data['name'] = None
        data['parent'] = None
        for dim in self.dimensions:
            dim_name = dim.id
            if dim_name in skip_dims:
                continue
            if dim_name in data.columns:
                df_codelist = self.codelist_series_by_id(dim_name)
                if df_codelist is None or len(df_codelist.index) == 0:
                    logger.warning(f'Did not found a codelist for DIM {dim_name}.')
                    continue
                logger.debug(f'LEFT JOIN dimension {dim_name} to existing Dataframe.')
                data = data.merge(
                    df_codelist,
                    left_on=dim_name,
                    right_index=True,
                    suffixes=(None, self.dim_suffix(dim_name)),
                )
                processed_dimensions.append(dim_name)
        return ProcessedDimensions(data, processed_dimensions)

    def melt_dimensions(self, processed_dim: ProcessedDimensions) -> pd.DataFrame:
        logger = get_dagster_logger()
        df = processed_dim.data
        dimension_dict_cols = []
        for dim in processed_dim.dimensions:
            dict_col_name = self.dim_dict(dim)
            logger.debug(f'Processing dict generation of DIM {dim}')
            df[dict_col_name] = df.apply(
                lambda row: {dim: {row[dim]: row[f'name_{dim}']}},
                axis=1,
            )
            # remove obsolete columns (memory management)
            df = df.drop([dim, f'name_{dim}', f'parent_{dim}'], axis=1)
            dimension_dict_cols.append(dict_col_name)
        # merge all dimension dicts
        df['grouping'] = df[dimension_dict_cols].apply(
            lambda r: {k: v for d in r for k, v in d.items()}, axis=1
        )
        # remove old columns after merge
        for dim in processed_dim.dimensions:
            dict_col_name = self.dim_dict(dim)
            df = df.drop(dict_col_name, axis=1)
            logger.debug(f'Dropped original column {dict_col_name}.')
        return df

    @property
    def source(self) -> str:
        if self.source_overwrite:
            return self.source_overwrite
        else:
            # Currently, I found no possibility to automatically get the source
            # from DSD in CH1.GWS. This can change in the future or in other
            # Dataflows.
            raise NotImplementedError


class SdmxMimeType(Enum):
    V1_STRUCTURE_JSON = (
        'application/vnd.sdmx.structure+json; charset=utf-8; version=1.0'
    )
    V2_CSV = 'application/vnd.sdmx.data+csv; charset=utf-8; version=2'


class SdmxAcceptLanguange(Enum):
    DE = 'de-ch'


class SdmxAcceptEncoding(Enum):
    GZIP = 'gzip'


class SdmxResource(ConfigurableResource):
    _BASE_URL: str


class StatsSwissResource(SdmxResource):
    _BASE_URL: str = 'https://disseminate.stats.swiss/rest'

    def dataflow(self, config: SdmxConfig, request_info: RequestsInfo) -> dict:
        logger = get_dagster_logger()
        url = config.dataflow_url(self._BASE_URL)
        logger.info(f"Loading data structure from {url}")
        request_header = {
            'Accept': SdmxMimeType.V1_STRUCTURE_JSON.value,
            'Accept-Language': SdmxAcceptLanguange.DE.value,
        }
        request_header |= request_info.headers
        response = requests.get(
            url,
            headers=request_header,
            params=dict(references='all'),
        )
        return response.json()

    def raw_data(
        self, config: SdmxConfig, request_info: RequestsInfo, **kwargs
    ) -> BytesIO:
        logger = get_dagster_logger()
        url = config.data_url(self._BASE_URL)
        logger.info(f"Loading data from {url}")
        request_header = {
            'Accept': SdmxMimeType.V2_CSV.value,
            'Accept-Language': SdmxAcceptLanguange.DE.value,
            'Accept-Encoding': SdmxAcceptEncoding.GZIP.value,
        }
        request_header |= request_info.headers
        response = requests.get(
            url,
            headers=request_header,
            # for testing purposes
            # params=dict(startPeriod='2024'),
            **kwargs,
        )
        return BytesIO(response.content)

    def process_raw_data(
        self,
        data: BytesIO,
        config: SdmxConfig,
        source_system: str,
        source_timestamp: dt.datetime,
    ) -> pd.DataFrame:
        df = pd.read_csv(data)

        # Use category dtype to safe storage
        # Witout this, static values will be saved as object (strings) and use
        # Memory again and again, for each row.
        df['source'] = pd.Categorical.from_codes(
            np.zeros(len(df), dtype='int8'), categories=[config.source]
        )
        df['_source_system'] = pd.Categorical.from_codes(
            np.zeros(len(df), dtype='int8'), categories=[source_system]
        )
        df['_source_timestamp'] = pd.Categorical.from_codes(
            np.zeros(len(df), dtype='int8'), categories=[source_timestamp]
        )
        df['_source_sequence'] = range(len(df))
        return df
