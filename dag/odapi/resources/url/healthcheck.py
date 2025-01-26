import requests
from dagster import ConfigurableResource
import os

class HealthCheckResource(ConfigurableResource):

    def _url(self, env_var: str) -> str:
        url = os.getenv(env_var)
        if url is None:
            raise ValueError(f'Environment variable {self.env_var} is not set')
        return url

    def ping_by_env(self, env_var: str) -> bool:
        try:
            response = requests.get(self._url(env_var))
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            return False
