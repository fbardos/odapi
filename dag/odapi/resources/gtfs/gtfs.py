import io
import json
import time
import zipfile
from typing import List
from typing import Optional
from typing import Tuple

import pandas as pd
import requests
from dagster import AssetObservation
from dagster import ConfigurableResource
from dagster import Definitions
from dagster import EnvVar
from dagster import MaterializeResult
from dagster import MetadataValue
from dagster import OpExecutionContext
from dagster import Output
from dagster import ResourceDependency
from dagster import asset
from dagster import load_assets_from_package_module
from dagster import op
from google.transit import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict
from sqlalchemy import INTEGER
from sqlalchemy import Column

from odapi.resources.ckan.ckan import GtfsOpenTransportDataCkanApi
from odapi.resources.minio.minio import GtfsMinio
from odapi.resources.minio.minio import MinioNoKeysFound


# First, define a resource to access the API
class GTFSRTResource(ConfigurableResource):
    api_key: str
    _BASE_URL = 'https://api.opentransportdata.swiss/gtfsrt2020'

    def request(self) -> Tuple[dict, float]:
        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(
            self._BASE_URL,
            headers={
                'Authorization': self.api_key,
            },
        )
        feed.ParseFromString(response.content)
        data = protobuf_to_dict(feed)
        _time = time.time()
        data['header']['_import_timestamp'] = _time

        # Sometimes the API returns empty stop_time_update (rarely, but it happens)
        data['entity'] = [
            entity
            for entity in data['entity']
            if entity.get('trip_update', {}).get('stop_time_update') is not None
        ]
        return data, _time


class GTFSResource(ConfigurableResource):
    """Combines the two resources"""

    ckan_resource: ResourceDependency[GtfsOpenTransportDataCkanApi]
    minio_resource: ResourceDependency[GtfsMinio]

    @property
    def _get_unloaded_resources(self) -> List[dict]:
        try:
            resources_dict = self.minio_resource.list_resource_keys
            resources_minio = list(resources_dict.keys())
        except MinioNoKeysFound:
            resources_minio = []
        unloaded_resources = []
        for resource in self.ckan_resource.list_resources_from_packages_matching_regex:
            if resource['identifier'] not in resources_minio:
                unloaded_resources.append(resource)
        return unloaded_resources

    @property
    def _oldest_unloaded_resource(self) -> Optional[dict]:
        return self.ckan_resource.oldest_resource_from_list_of_resources(
            self._get_unloaded_resources
        )

    @property
    def oldest_unloaded_resource_url(self) -> Optional[str]:
        if _resource := self._oldest_unloaded_resource:
            return _resource['url']
        else:
            return None

    @property
    def oldest_unloaded_resource_filename(self) -> Optional[str]:
        if _resource := self._oldest_unloaded_resource:
            return _resource['identifier']
        else:
            return None

    @property
    def oldest_unloaded_resource_zipfile_obj(self) -> io.BytesIO:
        _oldest_unloaded_resource_url = self.oldest_unloaded_resource_url
        assert isinstance(_oldest_unloaded_resource_url, str)
        try:
            response = requests.get(_oldest_unloaded_resource_url)
        except requests.exceptions.RequestException as e:
            raise Exception(
                f"Failed to download resource from {_oldest_unloaded_resource_url}"
            ) from e

        # buffer = zipfile.ZipFile(io.BytesIO(response.content))
        return io.BytesIO(response.content)

    def load_data_from_gtfs_export(
        self, minio_key: str, extracted_filename: str
    ) -> pd.DataFrame:
        zipfile_buffer = self.minio_resource.download_object(minio_key)
        with zipfile.ZipFile(zipfile_buffer, 'r') as zip:
            with zip.open(extracted_filename) as file:
                df = pd.read_csv(file)
                return df
