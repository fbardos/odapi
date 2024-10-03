from dagster import ConfigurableResource
import boto3
from typing import List
from typing import Dict
from typing import Optional
import pandas as pd
import io
import re
from botocore.exceptions import ClientError
import logging


class MinioNoKeysFound(Exception):
    pass


class Minio(ConfigurableResource):
    endpoint_url: str
    bucket_name: str
    access_key: str
    secret_key: str
    _PAGINATOR_CONFIG = {
        'PageSize': 1000,
        'MaxItems': 10000,
    }

    @property
    def client(self):
        return boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def list_keys(self, prefix: str, delimiter: str = '') -> Optional[List[str]]:
        """Source: https://airflow.apache.org/docs/apache-airflow/1.10.11/_modules/airflow/hooks/S3_hook.html#S3Hook.list_keys"""
        paginator = self.client.get_paginator('list_objects_v2')
        response = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=prefix,
            Delimiter=delimiter,
            PaginationConfig=self._PAGINATOR_CONFIG,
        )
        has_results = False
        keys = []
        for page in response:
            if 'Contents' in page:
                has_results = True
                for k in page['Contents']:
                    keys.append(k['Key'])
        if has_results:
            return keys

    def check_for_key(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            logging.info(e.response["Error"]["Message"])
            return False

    def load_bytes(self, bytes_data: io.BytesIO, key: str, replace: bool = False) -> None:
        """Source: https://airflow.apache.org/docs/apache-airflow/1.10.11/_modules/airflow/hooks/S3_hook.html#S3Hook.load_bytes"""
        if not replace and self.check_for_key(key):
            raise ValueError(f'The key {key} already exists.')
        self.client.upload_fileobj(bytes_data, self.bucket_name, key)

    def read_key(self, key: str) -> io.BytesIO:
        buffer = io.BytesIO()
        self.client.download_fileobj(Bucket=self.bucket_name, Key=key, Fileobj=buffer)
        buffer.seek(0)
        return buffer
        # obj = self.get_key(key)
        # return io.BytesIO(obj.get()['Body'].read())

    def download_object(self, key: str) -> io.BytesIO:
        buffer = io.BytesIO()
        self.client.download_fileobj(self.bucket_name, key, buffer)
        buffer.seek(0)
        return buffer


class GtfsMinio(Minio):
    _PREFIX = 'gtfs/gtfs'

    @property
    def list_resource_keys(self) -> Dict[str, str]:
        keys = self.list_keys(prefix=self._PREFIX)
        if not isinstance(keys, list):
            raise MinioNoKeysFound(f'No keys found in bucket {self.bucket_name} with prefix {self._PREFIX}')
        return {re.sub('^' + self._PREFIX + '/?', '', key): key for key in keys}

