from typing import Literal

import dagster as dg
import geopandas as gpd


class WFSResource(dg.ConfigurableResource):
    SFTP_DIR_PARENT: str = 'wfs'

    def load_layer(
        self, url: str, layer: str, engine: Literal['pyogrio', 'fiona'] = 'pyogrio'
    ) -> gpd.GeoDataFrame:
        return gpd.read_file(
            f'WFS:{url}',
            layer=layer,
            engine=engine,
        )
