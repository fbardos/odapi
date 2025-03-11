"""Uses STAT-TAB API from BFS

  - Documentation: https://www.bfs.admin.ch/bfs/de/home/dienstleistungen/forschung/api/api-pxweb.assetdetail.16944731.html
  - Catalog: https://www.bfs.admin.ch/bfs/de/home/dienstleistungen/forschung/stat-tab-online-datenrecherche.html

Inside the catalog, it is possible to filter by datasets containing municipalities.

After some testing, it would be convenient to load the .px files from URL instead of
calling the API, because the convertion to a pandas Dataframe does not need need
control over the return format (csv, json, ...).

"""

from typing import Tuple

from dagster import ConfigurableResource
from dagster import get_dagster_logger
from pandas import DataFrame
from pyaxis import pyaxis


class StatTabResource(ConfigurableResource):
    _ENCODING = 'utf-8'

    def _load_px_file(self, uri: str) -> dict:
        """Loads the actual PX file.

        Args:
            uri (str): URL (remote or local) to the PX file.

        """
        logger = get_dagster_logger()
        logger.info(f"Loading data from {uri}, encoding {self._ENCODING}")
        return pyaxis.parse(uri=uri, encoding=self._ENCODING)

    def load_data(self, uri: str) -> Tuple[DataFrame, dict]:
        """Returns data as pandas.DataFrame and metadata as dictionary."""
        px = self._load_px_file(uri=uri)
        return px['DATA'], px['METADATA']
