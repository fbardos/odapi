from cryptography.fernet import Fernet
from dagster import ConfigurableResource


# Symmetric encryption
class FernetCipher(ConfigurableResource):
    fernet_key: str

    @property
    def _cipher(self):
        return Fernet(self.fernet_key)

    def encrypt(self, data: bytes) -> bytes:
        return self._cipher.encrypt(data)

    def decrypt(self, data: bytes) -> bytes:
        return self._cipher.decrypt(data)
