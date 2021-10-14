from azure.core.exceptions import ResourceNotFoundError, ServiceResponseTimeoutError
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import Factory
from azureclient.rg import AZResourceGroup
from config.azconfig import AZDataPipelineConfig
from utilities.helper import countdown


class AZDataFactory:
    OPERATION_TIMEOUT = AZDataPipelineConfig.OPERATION_TIMEOUT
    WAIT_ATTEMPTS = AZDataPipelineConfig.WAIT_ATTEMPTS

    def __init__(self, adf_name: str = AZDataPipelineConfig.ADF_NAME, rg: AZResourceGroup = AZResourceGroup()):
        if not rg.exists():
            raise ResourceNotFoundError(message=f"The resource group ({rg.name}) does not exist")
        self._rg = rg
        self.adf_client = DataFactoryManagementClient(
            credential=self._rg.credential, subscription_id=self._rg.subscription_id
        )

        self.adf_name = adf_name

    def exists(self):
        try:
            if self.adf_client.factories.get(self._rg.name, self.adf_name):
                return True
        except ResourceNotFoundError:
            return False

    def create_or_update(
        self, location: str = AZDataPipelineConfig.RG_LOCATION, tags: dict = AZDataPipelineConfig.TAGS
    ):
        created_or_updated = "updated" if self.exists() else "created"

        tags = tags if tags else None
        factory = Factory(location=location, tags=tags)

        adf = self.adf_client.factories.create_or_update(self._rg.name, self.adf_name, factory)

        attempt_count = 0
        while adf.provisioning_state != "Succeeded":
            if attempt_count < self.WAIT_ATTEMPTS:
                adf = self.adf_client.factories.get(self._rg.name, self.adf_name)
                countdown(message="Countdown to next waiting attempt", time_sec=self.OPERATION_TIMEOUT)
                attempt_count += 1
                mes = (
                    f"The data factory ({self.adf_name}) is still being {created_or_updated}. "
                    f"Waiting attempt count: {attempt_count}"
                )
                print(f"\r\033[94m{mes}\033[0m", end="", flush=True)

            else:
                break

        if adf.provisioning_state != "Succeeded":
            raise ServiceResponseTimeoutError(message="CREATE operation timeout")

        print(f"\r", end="", flush=True)
        print(f"The data factory ({self.adf_name}) has been {created_or_updated}")

        return adf

    def delete(self):
        if self.exists():
            self.adf_client.factories.delete(self._rg.name, self.adf_name)
            print(f"The data factory ({self.adf_name}) has been deleted")

    def create_storage_link(self):
        pass


if __name__ == "__main__":
    is_to_test_run = True
    if is_to_test_run:
        rg = AZResourceGroup()
        if not rg.exists():
            rg.create_or_update()

        adf = AZDataFactory(rg=rg)
        if not adf.exists():
            adf.create_or_update()
    else:
        pass
