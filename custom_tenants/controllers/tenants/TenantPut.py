import json

from models.Tenant import TenantModel
from process.tenants.TenantsPut import TenantsPut

from core_utils.functions import UUID, lambda_decorator, tenant_setup, webhook_dispatch
from utils.auth.permissions import Permission
from utils.common import handle_payload_error


@tenant_setup
@webhook_dispatch("tenants_master", "update")
@lambda_decorator
def lambda_handler(event, context):
    user_id, tenant_id = event["user_id"], event["tenant_id"]
    payload = json.loads(event["body"])

    permission = Permission(
        user_id=user_id,
        tenant_id=tenant_id,
        event_uuid=UUID,
        module="admin",
        component_name="tenants",
        subcomponent="general",
        action="can_update",
    )
    permission.check_permissions()

    tenant_object = TenantModel(user_id, tenant_id)
    valid, code = tenant_object.tenant_put_validate_payload(payload)
    tenant_put_process = TenantsPut(tenant_object, user_id=user_id)
    handle_payload_error(valid, code)
    response = tenant_put_process.perform()

    return response
