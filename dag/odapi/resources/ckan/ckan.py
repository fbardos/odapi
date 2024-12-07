import re
import requests
from typing import List
from typing import Optional
import json
import datetime
from dagster import ConfigurableResource


class CkanApi(ConfigurableResource):
    _BASE_URL: str

    def get_package_by_id(self, id: str) -> dict:
        response = requests.get(
            '/'.join([self._BASE_URL, 'package_show']),
            params={
                'id': id
            }
        )
        return response.json()

    def get_resource_by_id(self, id: str) -> dict:
        response = requests.get(
            '/'.join([self._BASE_URL, 'resource_show']),
            params={
                'id': id
            }
        )
        return response.json()

    def get_resource_modified(self, id: str) -> datetime.datetime:
        resource = self.get_resource_by_id(id)
        modified = datetime.datetime.fromisoformat(resource.get('result', {}).get('modified'))
        if modified.tzinfo is None:
            modified = modified.replace(tzinfo=datetime.timezone.utc)  # Could be problematic
        return modified

    def get_resource_url(self, id: str) -> str:
        resource = self.get_resource_by_id(id)
        return resource.get('result', {}).get('download_url')

    def get_resource_byte_size(self, id: str) -> int:
        resource = self.get_resource_by_id(id)
        byte_size = resource.get('result', {}).get('byte_size')
        byte_size = int(byte_size)
        assert isinstance(byte_size, int), 'Byte size is not an integer'
        return byte_size


class OpenTransportDataCkanApi(CkanApi):
    api_key: str
    _BASE_URL: str = 'https://api.opentransportdata.swiss/ckan-api'

class OpenDataSwiss(CkanApi):
    _BASE_URL: str = 'https://opendata.swiss/api/3/action/'


class GtfsOpenTransportDataCkanApi(OpenTransportDataCkanApi):
    _PACKAGE_REGEX = r'timetable-\d{4}-gtfs2020'

    # capture group is used for extracting the date
    _RESOURCE_ID_REGEX = r'GTFS_FP\d{4}_(\d{4}-\d{2}-\d{2}).*\.zip'

    @property
    def _list_package_url(self) -> str:
        return '/'.join([self._BASE_URL, 'package_list'])

    @property
    def _GET_package_list(self) -> dict:
        response = requests.get(
            self._list_package_url,
            headers={
                'Authorization': self.api_key,
            }
        )
        _data = response.json()
        assert _data.get('success', False), 'Request failed'
        return _data

    def _filter_packages_by_regex(self) -> List[str]:
        response = self._GET_package_list
        slugs = response.get('result', [])
        return [slug for slug in slugs if re.match(self._PACKAGE_REGEX, slug)]

    @property
    def _package_show_url(self) -> str:
        return '/'.join([self._BASE_URL, 'package_show'])

    def _GET_package_by_slug(self, slug: str) -> dict:
        response = requests.get(
            self._package_show_url,
            headers={
                'Authorization': self.api_key,
            },
            params={
                'id': slug
            }
        )
        _data = response.json()
        assert _data.get('success', False), 'Request failed'
        return _data

    def list_resources_from_package(self, slug: str) -> List[dict]:
        response = self._GET_package_by_slug(slug)
        resources = response.get('result', {}).get('resources', [])
        return resources

    @property
    def list_resources_from_packages_matching_regex(self) -> List[dict]:
        _filtered_resource = []
        for slug in self._filter_packages_by_regex():
            resources = self.list_resources_from_package(slug)
            for resource in resources:
                if re.match(self._RESOURCE_ID_REGEX, resource['identifier']):
                    _filtered_resource.append(resource)
        return _filtered_resource

    def oldest_resource_from_list_of_resources(self, resources: List[dict]) -> Optional[dict]:
        _resources = resources.copy()
        if len(_resources) > 0:
            _resources.sort(
                key=lambda x: re.match(self._RESOURCE_ID_REGEX, x.get('identifier', 'GTFS_FP1970_1970-01-01.zip')).group(1),
                reverse=False
            )
            return _resources[0]
        else:
            return None


    # # TODO: this should be in GTFS class
    # @property
    # def latest_resource(self) -> dict:
        # """Returns a URL for the latest GTFS data."""

        # # Extract all resources from the packages
        # # Filter out with two regexes (package and resource identifier)
        # _filtered_resource = self.list_resources_from_packages_matching_regex

        # # Extract timestamp and sort by date
        # _filtered_resource.sort(
            # key=lambda x: re.match(self._RESOURCE_ID_REGEX, x.get('identifier', 'GTFS_FP1970_1970-01-01.zip')).group(1),
            # reverse=True
        # )
        # return _filtered_resource[0]

    # @property
    # def latest_resource_url(self) -> str:
        # return self.latest_resource.get('url', None)
