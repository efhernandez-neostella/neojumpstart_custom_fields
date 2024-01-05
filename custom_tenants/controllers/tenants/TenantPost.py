import json

import boto3
from models.Tenant import TenantModel
from process.tenants.TenantCreation import TenantCreationProcess

from core_utils.functions import (
    SERVICE_NAME,
    UUID,
    lambda_decorator,
    tenant_setup,
    webhook_dispatch,
)
from utils.auth.permissions import Permission
from utils.common import handle_payload_error

LAMBDA_CLIENT = boto3.client("lambda")


@tenant_setup
@webhook_dispatch("tenants_master", "create")
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
        action="can_create",
    )
    permission.check_permissions()

    tenant_object = TenantModel(user_id, tenant_id)
    valid, code = tenant_object.validate_payload(payload)
    tenant_post_process = TenantCreationProcess(tenant_object)
    handle_payload_error(valid, code)
    tenant_post_process.valid_unique_fields()
    tenant_id = tenant_object.create_tenant_record(user_id=user_id)
    payload["tenant_id"] = tenant_id
    event["body"] = json.dumps(payload)

    LAMBDA_CLIENT.invoke(
        FunctionName=f"{SERVICE_NAME}-TenantCreation",
        InvocationType="Event",
        Payload=json.dumps(event),
    )

    return (
        200,
        {
            "correlation_id": UUID,
            "tenant_id": tenant_id,
            "result": "Tenant process creation initiated",
        },
    )
