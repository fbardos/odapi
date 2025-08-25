from dagster import ConfigurableResource


class RequestsInfo(ConfigurableResource):
    user_from: str
    user_version: str = '1.0'
    user_domain: str = '+https://odapi.bardos.dev'
    user_product: str = 'odapi'

    @property
    def headers(self) -> dict:
        # Most enterprise proxies will ignore the From: header.
        # Therefore, write the information directly into the User-Agent header.
        return {
            'User-Agent': f'{self.user_product}/{self.user_version} ({self.user_domain}; {self.user_from})',
        }
