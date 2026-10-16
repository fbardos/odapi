from dagster import ConfigurableResource
from dagster import get_dagster_logger


class RequestsInfo(ConfigurableResource):
    user_from: str
    user_version: str = '1.0'
    user_domain: str = '+https://odapi.bardos.dev'
    user_product: str = 'odapi'

    @property
    def headers(self) -> dict:
        # Most enterprise proxies will ignore the From: header.
        # Therefore, write the information directly into the User-Agent header.
        user_agent = {
            'User-Agent': (
                f'{self.user_product}/{self.user_version} '
                f'({self.user_domain}; {self.user_from})'
            ),
        }
        logger = get_dagster_logger()
        logger.debug(f'Custom User-Agent: {user_agent}')
        return user_agent
