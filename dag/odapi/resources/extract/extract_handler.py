import sys
from gzip import GzipFile
import gzip
import io
import pandas as pd
from dagster import ConfigurableResource
from dagster import ResourceDependency
from odapi.resources.minio.minio import Minio
from odapi.resources.crypto.fernet import FernetCipher
import pandas as pd


class ExtractHandler(ConfigurableResource):
    resource_minio: ResourceDependency[Minio]
    resource_fernet: ResourceDependency[FernetCipher]
    
    def _convert_to_parquet(self, df: pd.DataFrame) -> io.BytesIO:
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
        buffer.seek(0)
        return buffer
    
    def _load_dataframe(self, buffer: gzip.GzipFile) -> pd.DataFrame:
        return pd.read_parquet(buffer, engine='pyarrow')

    def _encrypt_data(self, buffer: io.BytesIO) -> io.BytesIO:
        encrypted_data = self.resource_fernet.encrypt(buffer.getvalue())
        encrypted_buffer = io.BytesIO(encrypted_data)
        encrypted_buffer.seek(0)
        return encrypted_buffer
    
    def _decrypt_data(self, buffer: io.BytesIO) -> io.BytesIO:
        return io.BytesIO(self.resource_fernet.decrypt(buffer.getvalue()))

    def _compress_data(self, buffer: io.BytesIO) -> io.BytesIO:
        compressed_buffer = io.BytesIO()
        with GzipFile(fileobj=compressed_buffer, mode='wb') as f:
            f.write(buffer.getvalue())
        compressed_buffer.seek(0)
        return compressed_buffer
    
    def _decompress_data(self, buffer: io.BytesIO) -> gzip.GzipFile:
        return gzip.open(buffer, 'rb')

    def _upload_to_minio(self, buffer: io.BytesIO, file_key: str) -> None:
        # key = '/'.join([self.ckan_resource.model_name, f'extracted_data_{execution_date.isoformat()}.fernet'])
        self.resource_minio.load_bytes(buffer, key=file_key)
    
    def _load_from_minio(self, file_key: str) -> io.BytesIO:
        return self.resource_minio.read_key(key=file_key)

    def _get_size_bytes(self, buffer: io.BytesIO) -> int:
        return sys.getsizeof(buffer.getvalue())

    def write_data(self, file_key: str, data: pd.DataFrame) -> int:
        buffer = self._convert_to_parquet(data)
        compressed_buffer = self._compress_data(buffer)
        encrypted_buffer = self._encrypt_data(compressed_buffer)
        self._upload_to_minio(encrypted_buffer, file_key)
        return self._get_size_bytes(compressed_buffer) 

    def read_data(self, file_key: str) -> pd.DataFrame:
        encrypted_buffer = self._load_from_minio(file_key)
        decrypted_buffer = self._decrypt_data(encrypted_buffer)
        decompressed_buffer = self._decompress_data(decrypted_buffer)
        return self._load_dataframe(decompressed_buffer)
