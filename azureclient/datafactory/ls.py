from azure.mgmt.datafactory.models import (
    AzureStorageLinkedService,
    LinkedServiceResource,
)
from azureclient.datafactory.adf import AZDataFactory
from azureclient.rg import AZResourceGroup
from azureclient.storage.blob import BlobStorageContainer
from azureclient.storage.sa import AZStorageAccount

CONN_STR = AZStorageAccount().conn_string


def print_properties(props):
    """Print a ResourceGroup properties instance."""
    if props and hasattr(props, "provisioning_state") and props.provisioning_state:
        print("\tProperties:")
        print("\t\tProvisioning State: {}".format(props.provisioning_state))
    print("\n\n")


def print_item(group):
    """Print an Azure object instance."""
    print("\tName: {}".format(group.name))
    print("\tId: {}".format(group.id))
    if hasattr(group, "location"):
        print("\tLocation: {}".format(group.location))
    if hasattr(group, "tags"):
        print("\tTags: {}".format(group.tags))
    if hasattr(group, "properties"):
        print_properties(group.properties)


class ILinkedService:
    pass


class BlobStorageLS(ILinkedService):
    def __init__(self, ls_name: str, blob: BlobStorageContainer, adf: AZDataFactory, rg: AZResourceGroup):
        self.ls_name = ls_name
        self._blob = blob
        self._adf = adf

        self.ls_azure_storage = LinkedServiceResource(
            properties=AzureStorageLinkedService(connection_string=self._blob.conn_string)
        )

        self._link_service: Optional[Li]

    def exists(self) -> bool:
        ls = self._adf.adf_client.linked_services.get(
            resource_group_name=rg.name, factory_name=adf.adf_name, linked_service_name=self.ls_name
        )

        print_item(self._link_service)
        return True if ls else False

    def create(self):
        if not self.exists():
            self._adf.adf_client.linked_services.create_or_update(
                resource_group_name=rg.name,
                factory_name=adf.adf_name,
                linked_service_name=self.ls_name,
                linked_service=self.ls_azure_storage,
            )


if __name__ == "__main__":
    rg = AZResourceGroup()
    adf = AZDataFactory()
    blob = BlobStorageContainer()
    blob_ls = BlobStorageLS("hello", blob, adf, rg)
