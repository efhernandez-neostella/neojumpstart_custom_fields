import json

from entities_utils import is_uuid_valid, validate_payload
from models.Entity import EntityModel
from process.entities.EntityPut import EntityPut

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
        component_name="entity_configuration",
        subcomponent="entity",
        action="can_update",
    )
    permission.check_permissions()


def _get_entity_id(parameters: dict) -> str:
    if entity_id := parameters.get("entity_id"):
        if is_uuid_valid(entity_id):
            return entity_id
    raise ControllerException(
        400, {"correlation_id": UUID, "code": "EntityPut.InvalidEntityId"}
    )


def _validate_payload(payload: dict) -> None:
    validate_payload(payload, ["name"])


@tenant_setup
@webhook_dispatch("entities_master", "create")
@lambda_decorator
def lambda_handler(event, context):
    user_id, tenant_id = event["user_id"], event["tenant_id"]
    payload = json.loads(event["body"])
    entity_id = _get_entity_id(event["pathParameters"])
    _check_permissions(user_id, tenant_id)
    if not payload:
        raise ControllerException(
            400,
            {
                "error": "Empty body.",
                "code": "CustomEntities.InvalidBody",
            },
        )
    _validate_payload(payload)
    entity_object = EntityModel(user_id, tenant_id)
    entity_put_process = EntityPut(entity_object, payload, entity_id)
    response = entity_put_process.perform()
    return response