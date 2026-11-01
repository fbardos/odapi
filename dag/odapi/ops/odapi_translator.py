import datetime as dt
import re
from dataclasses import dataclass
from typing import Literal


@dataclass
class OdapiTranslator:
    model_name: str
    file_type: Literal['csv', 'parquet'] = 'csv'

    _WWW_PREFIX: str = 'https://files.bardos.dev/odapi'
    _DIR_NAME: str = 'opendata_swiss'
    _DEFAULT_TIMESTAMP_FORMAT: str = '%Y-%m-%dT%H-%M-%SZ'
    _DEFAULT_TIMESTAMP_REGEX: str = r'\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z'

    @property
    def partition_name(self) -> str:
        return f'part_{self.model_name}'

    @property
    def job_name_web(self) -> str:
        return f'job_web_{self.model_name}'

    @property
    def filetype_ending(self) -> str:
        return f'.{self.file_type}'

    @property
    def asset_name(self) -> str:
        return f'odch_{self.model_name}'

    @property
    def dlt_pipeline_name(self) -> str:
        return f'pipe_{self.model_name}'

    @property
    def sensor_name_web(self) -> str:
        return f'sensor_web_{self.model_name}'

    @property
    def sensor_name_sftp(self) -> str:
        return f'sensor_sftp_{self.model_name}'

    @property
    def dir(self) -> str:
        return '/'.join([self._DIR_NAME, self.model_name])

    def path(self, timestamp: dt.datetime, file_ending: str) -> str:
        return '/'.join([self.dir, self.filename(timestamp, file_ending)])

    def filename(self, timestamp: dt.datetime, file_ending: str = '.csv.xz') -> str:
        return f'{self.model_name}_{timestamp.strftime(self._DEFAULT_TIMESTAMP_FORMAT)}{file_ending}'

    def extract_timestamp_from_filename(self, filename: str) -> dt.datetime:
        match = re.search(
            self._DEFAULT_TIMESTAMP_REGEX,
            filename,
        )
        if not match:
            raise ValueError(f'No timestamp found in filename: {filename}')
        return dt.datetime.strptime(
            match.group(),
            self._DEFAULT_TIMESTAMP_FORMAT,
        )

    def www_url(self, partition_key: str) -> str:
        return '/'.join([self._WWW_PREFIX, self.dir, partition_key])

    def www_url_from_remote_path(self, remote_path: str) -> str:
        partition = Path(remote_path).name
        return self.www_url(partition_key=partition)
