import geopandas as gpd
import requests
from dagster import ConfigurableResource


class OGCResource(ConfigurableResource):
    base_url: str

    SFTP_DIR_PARENT: str = 'ogc'

    def load_collection(self, collection: str) -> gpd.GeoDataFrame:
        features = []
        offset = 0
        page_size = 1000
        crs = 4326
        seen_ids = set()
        url = '/'.join([self.base_url, 'collections', collection, 'items'])

        with requests.Session() as session:
            while True:
                response = session.get(
                    url,
                    params={
                        'limit': page_size,
                        'offset': offset,
                        'sortby': 'fid',
                        'crs': crs,
                    },
                    headers={'Accept': 'application/geo+json'},
                    timeout=120,
                )
                response.raise_for_status()
                batch = response.json()['features']

                if not batch:
                    break

                ids = [f["id"] for f in batch]
                if len(ids) != len(set(ids)) or seen_ids.intersection(ids):
                    raise RuntimeError('Duplicate features detected during pagination')

                seen_ids.update(ids)
                features.extend(batch)
                offset += len(batch)

                print(f'Retrieved {offset} features')

        gdf = gpd.GeoDataFrame.from_features(features, crs=f'EPSG:{crs}')

        # gdf = gpd.read_file(
        #     f'OAPIF:{url}',
        #     engine='pyogrio',
        #     PAGE_SIZE=1000,
        # )
        return gdf
