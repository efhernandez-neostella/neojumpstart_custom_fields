from models.Field import FieldModel
from process.fields.FieldGet import FieldGet

from core_utils.functions import UUID, lambda_decorator, tenant_setup
from utils.auth.permissions import Permission


@tenant_setup
@lambda_decorator
def lambda_handler(event, context):
    user_id, tenant_id = event["user_id"], event["tenant_id"]
    payload = event["pathParameters"]

    permission = Permission(
        user_id=user_id,
        tenant_id=tenant_id,
        event_uuid=UUID,
        module="admin",
        component_name="object_configurations",
        subcomponent="fields",
        action="can_read",
    )
    permission.check_permissions()

    field_object = FieldModel(user_id, tenant_id)
    field_object.validate_id_payload(payload=payload)
    field_get_process = FieldGet(field_object)
    response = field_get_process.perform()

    return response
