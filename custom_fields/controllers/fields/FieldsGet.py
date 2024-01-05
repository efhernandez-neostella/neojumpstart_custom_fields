from models.Field import FieldModel
from process.fields.FieldsGet import FieldsGet

from core_utils.functions import UUID, lambda_decorator, tenant_setup
from utils.auth.permissions import Permission
from utils.common import handle_payload_error


@tenant_setup
@lambda_decorator
def lambda_handler(event, context):
    user_id, tenant_id = event["user_id"], event["tenant_id"]
    payload = event["queryStringParameters"]
    if payload is None:
        payload = {}

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
    valid, code = field_object.fields_get_validate_payload(payload)
    handle_payload_error(valid, code)
    field_get_process = FieldsGet(field_object)
    response = field_get_process.perform()

    return response
