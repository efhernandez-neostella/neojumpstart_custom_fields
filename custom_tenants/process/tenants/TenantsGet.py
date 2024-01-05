from models.Tenant import TenantModel

from core_utils.functions import UUID


class TenantsGet:
    def __init__(self, tenant_object: "TenantModel", time_zone: str):
        self.tenant_object: "TenantModel" = tenant_object
        self.time_zone: str = time_zone

    def perform(self):
        tenants = self.tenant_object.tenants_get(self.time_zone)

        return (200, {"correlation_id": UUID, "tenants": tenants})
