import json

from custom_endpoint_utils import check_entity, get_entity_name
from models.CustomObject import CustomObjectModel
from process.ObjectPost import ObjectCreate

from core_utils.functions import RESOURCE_METHOD, UUID, lambda_decorator, tenant_setup
from utils.auth.permissions import Permission


def _check_permissions(user_id: str, tenant_id: str, entity_name: str) -> None:
    permission = Permission(
        user_id=user_id,
        tenant_id=tenant_id,
        event_uuid=UUID,
        module=entity_name,
        component_name="general",
        subcomponent="general",
        action="can_create",
    )
    permission.check_permissions()


@tenant_setup
@lambda_decorator
def lambda_handler(event, context):
    user_id, tenant_id = event["user_id"], event["tenant_id"]
    payload = json.loads(event["body"])
    path_params = event["pathParameters"]
    entity_id, entity_name = get_entity_name(path_params, RESOURCE_METHOD)
    check_entity(entity_name)
    _check_permissions(user_id, tenant_id, entity_name)
    custom_object = CustomObjectModel(user_id, tenant_id, entity_name, entity_id)
    custom_create_process = ObjectCreate(custom_object, payload, path_params)
    response = custom_create_process.perform()
    return response
