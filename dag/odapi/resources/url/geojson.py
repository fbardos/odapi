import geopandas as gpd
from dagster import ConfigurableResource

class GeoJsonResource(ConfigurableResource):
    _URL: str

    def load(self) -> gpd.GeoDataFrame:
        return gpd.read_file(self._URL)


class SwissboundariesTill2015(GeoJsonResource):
    _URL: str = 'https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json'
