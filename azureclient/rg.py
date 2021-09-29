from azure.core.exceptions import ServiceResponseTimeoutError, ResourceNotFoundError
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient

from config.azconfig import AzureConfig, AZDataPipelineConfig


class AZResourceGroup:
    OPERATION_TIMEOUT = AZDataPipelineConfig.OPERATION_TIMEOUT
    WAIT_ATTEMPTS = AZDataPipelineConfig.WAIT_ATTEMPTS

    def __init__(self, subscription_id: str = AzureConfig.SUBSCRIPTION_ID, rg_name: str = AZDataPipelineConfig.RG_NAME):
        self.credential = AzureCliCredential()
        self._rg_client = ResourceManagementClient(credential=self.credential, subscription_id=subscription_id)
        self.exist: bool = self._rg_client.resource_groups.check_existence(rg_name)
        self.subscription_id = subscription_id
        self.name = rg_name

    def create_or_update(self, location: str = AZDataPipelineConfig.RG_LOCATION,
                         tags: dict = AZDataPipelineConfig.TAGS):
        created_or_updated = "updated" if self.exist else "created"

        params = {"location": location}
        if tags:
            params['tags'] = tags

        rg_result = self._rg_client.resource_groups.create_or_update(self.name, params)
        if rg_result:
            self.exist = True
            print(f'The resource group ({self.name}) has been {created_or_updated}.')
        else:
            if self._rg_client.resource_groups.check_existence(self.name):
                raise ResourceNotFoundError

        return rg_result

    def delete(self):
        if self.exist:
            result = self._rg_client.resource_groups.begin_delete(self.name)
            attempt_count = 0
            while not result.done():
                if attempt_count < self.WAIT_ATTEMPTS:
                    result.wait(self.OPERATION_TIMEOUT)
                    attempt_count += 1
                    mes = f'The resource group ({self.name}) is still being deleted. ' \
                          f'Waiting attempt count: {attempt_count}'
                    print(f'\r\033[94m{mes}\033[0m', end='', flush=True)

                else:
                    break
            if not result.done():
                raise ServiceResponseTimeoutError(message="DELETE operation timeout")

            self.exist = False
            print(f'\r ', end='', flush=True)
            print(f'The resource group ({self.name}) has been deleted')

    @property
    def location(self):
        if self.exist:
            return self._rg_client.resource_groups.get().location

    @property
    def tags(self):
        if self.exist:
            return self._rg_client.resource_groups.get().tags


if __name__ == '__main__':
    is_to_test_run = True
    is_to_create = True
    if is_to_test_run:
        rg = AZResourceGroup()
        if rg.exist:
            rg.delete()
        else:
            if is_to_create:
                rg.create_or_update()

    else:
        pass
