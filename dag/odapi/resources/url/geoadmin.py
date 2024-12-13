"""
    Contains the resources to download data from data.geo.admin.ch.

    Uses the STAC API [1] to retrieve the artefact URLs for a specific collection.
    Then downloads the shapefiles and return a GeoDataFrame.

    Data can manually be explored here:
      - Browser: https://data.geo.admin.ch/browser/index.html

    [1] https://data.geo.admin.ch/api/stac/static/spec/v0.9/api.html

"""
import requests
from dagster import ConfigurableResource
from dagster import get_dagster_logger
from typing import Optional
import geopandas as gpd
from dataclasses import dataclass
import datetime as dt
import pandas as pd
import re


@dataclass
class DownloadAssets:
    asset_key: str
    asset_type: str
    asset_href: str
    asset_epsg: str
    asset_created: dt.datetime
    asset_updated: dt.datetime
    feature_datetime: dt.datetime

class GeoAdminResource(ConfigurableResource):

    _STAC_URL = 'http://data.geo.admin.ch/api/stac/v0.9'

    def get_features_by_collection(self, collection: str) -> dict:
        url = f'{self._STAC_URL}/collections/{collection}/items'
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise Exception(f'Error: {e}')
        return response.json()

    def download_all_sources(
        self,
        collection: str,
        feature_id_regex: Optional[str] = None,
        artefact_type: Optional[str] = None,
        asset_key_regex: Optional[str] = None,
        layer: Optional[str] = None,
    ) -> gpd.GeoDataFrame:
        logger = get_dagster_logger()
        feature_collection = self.get_features_by_collection(collection=collection)

        # FILTER feature_id_regex
        filtered_features = []
        if feature_id_regex:
            for feature in feature_collection['features']:
                if re.match(feature_id_regex, feature['id']):
                    filtered_features.append(feature)
        else:
            filtered_features = feature_collection.get('features', [])

        # FILTER asset_key_regex
        filtered_assets = []
        if asset_key_regex is None:
            asset_key_regex = r'.*'
        for feature in filtered_features:
            for asset_key, asset in feature['assets'].items():
                if re.match(asset_key_regex, asset_key):
                    filtered_assets.append(
                        DownloadAssets(
                            asset_key=asset_key,
                            asset_type=asset['type'],
                            asset_href=asset['href'],
                            asset_epsg=asset['proj:epsg'],
                            asset_created=dt.datetime.strptime(asset['created'], '%Y-%m-%dT%H:%M:%S.%fZ'),
                            asset_updated=dt.datetime.strptime(asset['updated'], '%Y-%m-%dT%H:%M:%S.%fZ'),
                            feature_datetime=dt.datetime.strptime(
                                feature['properties']['datetime'], '%Y-%m-%dT%H:%M:%SZ'
                            ),
                        )
                    )

        # FILTER artefact_type
        if artefact_type:
            filtered_assets = [
                asset for asset in filtered_assets if asset.asset_type == artefact_type
            ]

        # After filtering, extract all the download links from the remaining features
        extracted_gdfs = []
        for asset in filtered_assets:
            logger.info(f'Downloading {asset.asset_key} from {asset.asset_href}')
            response = requests.get(asset.asset_href)
            with open('temp.gpkg', 'wb') as file:
                file.write(response.content)
            gdf = gpd.read_file('temp.gpkg', layer=layer)

            gdf['duedate'] = asset.feature_datetime
            extracted_gdfs.append(gdf)
        merged_gdf = pd.concat(extracted_gdfs)
        return gpd.GeoDataFrame(merged_gdf)

