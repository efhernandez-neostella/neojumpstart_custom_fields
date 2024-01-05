from custom_endpoint_utils import eval_bool, get_entity_name
from models.CustomObject import CustomObjectModel
from process.ListGet import ObjectList

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
        action="can_read",
    )
    permission.check_permissions()


def _get_parameters(query_params: dict) -> dict:
    parsed_params = {}
    for param, value in query_params.items():
        if param == "is_active":
            is_active = eval_bool(value)
            if isinstance(is_active, bool):
                parsed_params[param] = is_active

    return parsed_params


@tenant_setup
@lambda_decorator
def lambda_handler(event, context):
    user_id, tenant_id = event["user_id"], event["tenant_id"]
    path_params = event["pathParameters"]

    entity_id, entity_name = get_entity_name(path_params, RESOURCE_METHOD)
    _check_permissions(user_id, tenant_id, entity_name)

    query_params = event.get("queryStringParameters") or {}
    parameters = _get_parameters(query_params)

    custom_object = CustomObjectModel(user_id, tenant_id, entity_name, entity_id)
    custom_list_get_process = ObjectList(custom_object, parameters, path_params)
    response = custom_list_get_process.perform()
    return response
