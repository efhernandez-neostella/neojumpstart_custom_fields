from models.Tenant import TenantModel

from core_utils.functions import UUID


class TenantGet:
    def __init__(self, tenant_object: "TenantModel", time_zone: str):
        self.tenant_object: "TenantModel" = tenant_object
        self.time_zone: str = time_zone

    def perform(self):
        self.tenant_object.tenant_exist()
        tenant = self.tenant_object.tenant_get(time_zone=self.time_zone)

        return (200, {"correlation_id": UUID, "tenant": tenant})
