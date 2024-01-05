import json

from fields_utils import validate_payload
from models.Field import FieldModel
from process.fields.FieldPut import FieldPut

from core_utils.functions import UUID, lambda_decorator, tenant_setup, webhook_dispatch
from utils.auth.permissions import Permission
from utils.common import is_uuid_valid
from utils.exceptions.controller_exceptions import ControllerException


def _check_permissions(user_id: str, tenant_id: str) -> None:
    permission = Permission(
        user_id=user_id,
        tenant_id=tenant_id,
        event_uuid=UUID,
        module="admin",
        component_name="object_configurations",
        subcomponent="fields",
        action="can_update",
    )
    permission.check_permissions()


def _validate_payload(payload: dict) -> None:
    validate_payload(payload, ["name"])


def _get_field_id(parameters: dict) -> str:
    if field_id := parameters.get("custom_field_id"):
        if is_uuid_valid(field_id):
            return field_id
    raise ControllerException(400, {"correlation_id": UUID, "code": "FieldPut.InvalidFieldId"})


@tenant_setup
@webhook_dispatch("custom_fields_master", "update")
@lambda_decorator
def lambda_handler(event, context):
    user_id, tenant_id = event["user_id"], event["tenant_id"]
    payload = json.loads(event["body"])
    field_id = _get_field_id(event["pathParameters"])
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
    field_put_process = FieldPut(field_object, payload, field_id)
    response = field_put_process.perform()

    return response
