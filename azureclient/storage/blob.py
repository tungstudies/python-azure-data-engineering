from typing import Optional

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

from azureclient.storage.sa import AZStorageAccount
from config.azconfig import AZDataPipelineConfig

CONN_STR = AZStorageAccount().conn_string


class BlobStorageContainer:
    def __init__(self, container_name: str = AZDataPipelineConfig.STORAGE_CONTAINER_NAME, conn_str: str = CONN_STR):
        self.container_name = container_name
        self._conn_str = conn_str
        self._storage_client: Optional[BlobServiceClient] = BlobServiceClient.from_connection_string(conn_str=conn_str)
        self._container_client: Optional[ContainerClient] = None

    def exists(self):
        self._container_client = self._storage_client.get_container_client(self.container_name)
        return self._container_client.exists()

    def create(self):
        if not self.exists():
            self._container_client = self._storage_client.create_container(self.container_name)
            if self._container_client:
                print(f"The storage container ({self.container_name}) has been created.")
                return self._container_client

    def delete(self):
        if self.exists():
            self._container_client.delete_container()
            print(f"The storage container ({self.container_name}) has been deleted.")

    def upload_file(self, filepath: str):
        pass


if __name__ == '__main__':
    bsc = BlobStorageContainer()
    bsc.create()
