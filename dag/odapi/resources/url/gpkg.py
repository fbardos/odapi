from typing import Optional
from typing import Dict
import geopandas as gpd
from dagster import ConfigurableResource


# Currently, loading a shapefile directly from URL inside a docker containers
# gives me a segmentation fault. Switched to geopackage (gpkg).
class GpkgResource(ConfigurableResource):
    _URL: str
    _LAYERS: Dict[str, str]

    def load(self, layer: str, custom_url: Optional[str] = None) -> gpd.GeoDataFrame:
        if custom_url:
            return gpd.read_file(custom_url, layer=layer)
        else:
            return gpd.read_file(self._URL, layer=layer)


class Swissboundaries(GpkgResource):
    _URL: str = 'https://data.geo.admin.ch/ch.swisstopo.swissboundaries3d/swissboundaries3d_2024-01/swissboundaries3d_2024-01_2056_5728.gpkg.zip'
    _LAYERS: Dict[str, str] = {
        'gemeinde': 'tlm_hoheitsgebiet',
        'bezirk':   'tlm_bezirksgebiet',
        'kanton':   'tlm_kantonsgebiet',
        'land':     'tlm_landesgebiet',
    }

    def build_url(self, year: int) -> str:
        return self._URL.replace('2024', str(year))

    def load_gemeinde(self, year: int) -> gpd.GeoDataFrame:
        return self.load(layer=self._LAYERS['gemeinde'], custom_url=self.build_url(year))
    
    def load_bezirk(self, year: int) -> gpd.GeoDataFrame:
        return self.load(layer=self._LAYERS['bezirk'], custom_url=self.build_url(year))
    
    def load_kanton(self, year: int) -> gpd.GeoDataFrame:
        return self.load(layer=self._LAYERS['kanton'], custom_url=self.build_url(year))
    
    def load_land(self, year: int) -> gpd.GeoDataFrame:
        return self.load(layer=self._LAYERS['land'], custom_url=self.build_url(year))
