from models.Tenant import TenantModel
from process.tenants.TenantGet import TenantGet

from core_utils.functions import UUID, lambda_decorator, tenant_setup
from utils.auth.permissions import Permission
from utils.common import get_user_timezone, handle_payload_error


@tenant_setup
# @webhook_dispatch('tenants_master', 'create') # TODO: add tenants_master to configuration.json
@lambda_decorator
def lambda_handler(event, context):
    user_id, tenant_id = event["user_id"], event["tenant_id"]
    payload = event["pathParameters"]
    if payload is None:
        payload = {}

    permission = Permission(
        user_id=user_id,
        tenant_id=tenant_id,
        event_uuid=UUID,
        module="admin",
        component_name="tenants",
        subcomponent="general",
        action="can_read",
    )
    permission.check_permissions()
    time_zone = get_user_timezone(user_id=user_id)
    tenant_object = TenantModel(user_id, tenant_id)
    valid, code = tenant_object.tenant_get_validate_payload(payload)
    tenant_get_process = TenantGet(tenant_object, time_zone)
    handle_payload_error(valid, code)
    response = tenant_get_process.perform()

    return response
