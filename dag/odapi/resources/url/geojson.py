from io import BytesIO
from zipfile import ZipFile

import geopandas as gpd
import requests
from dagster import ConfigurableResource


class GeoJsonResource(ConfigurableResource):
    _URL: str

    def load(self) -> gpd.GeoDataFrame:
        return gpd.read_file(self._URL)

    def load_from_zip(self, path_in_archive: str) -> gpd.GeoDataFrame:
        """
        Download a ZIP file from self._URL in memory and load a GeoJSON file
        from inside it as a GeoDataFrame.

        Parameters
        ----------
        geojson_path : str
            Path to the GeoJSON file inside the ZIP archive.

        Returns
        -------
        geopandas.GeoDataFrame
        """
        response = requests.get(self._URL, timeout=60)
        response.raise_for_status()

        with ZipFile(BytesIO(response.content)) as zip_file:
            if path_in_archive not in zip_file.namelist():
                available = "\n".join(zip_file.namelist())
                raise FileNotFoundError(
                    f'"{path_in_archive}" not found in ZIP archive. '
                    f'Available files:\n{available}'
                )

            with zip_file.open(path_in_archive) as geojson_file:
                return gpd.read_file(geojson_file)


class SwissboundariesTill2015(GeoJsonResource):
    _URL: str = (
        'https://data.geo.admin.ch/ch.bfs.historisierte-administrative_grenzen_g0/historisierte-administrative_grenzen_g0_1850-2015/historisierte-administrative_grenzen_g0_1850-2015_gemeinde_2056.json'
    )


class QuartierBoundaries(GeoJsonResource):
    _URL: str = 'https://dam-api.bfs.admin.ch/hub/api/dam/assets/30487002/master'
