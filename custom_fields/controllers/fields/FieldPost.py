import json
from fields_utils import validate_payload
from models.Field import FieldModel
from process.fields.FieldsPost import FieldsPost
from core_utils.functions import (UUID, lambda_decorator, tenant_setup,
                                  webhook_dispatch)
from utils.auth.permissions import Permission
from utils.exceptions.controller_exceptions import ControllerException


def _check_permissions(user_id: str, tenant_id: str) -> None:
    permission = Permission(
        user_id=user_id,
        tenant_id=tenant_id,
        event_uuid=UUID,
        module="admin",
        component_name="object_configurations",
        subcomponent="fields",
        action="can_create",
    )
    permission.check_permissions()


def _validate_payload(payload: dict) -> None:
    validate_payload(payload, ["name"])


@tenant_setup
@webhook_dispatch('custom_fields_master', 'create')
@lambda_decorator
def lambda_handler(event, context):
    user_id, tenant_id = event['user_id'], event['tenant_id']
    payload = json.loads(event['body'])
    _check_permissions(user_id, tenant_id)
    if not payload:
        raise ControllerException(
            400,
            {
                "error": "Empty body.",
                "code": "CustomFields.InvalidBody",
            },
        )
    _validate_payload(payload)
    field_object = FieldModel(user_id, tenant_id)
    field_post_process = FieldsPost(field_object, payload)
    response = field_post_process.perform()

    return response
