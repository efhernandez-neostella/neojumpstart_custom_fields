import json

from entities_utils import (
    IS_ACTIVE_FIELD_PAYLOAD,
    NAME_FIELD_PAYLOAD,
    to_view_format,
    validate_payload,
)
from models.Entity import EntityModel
from models.Field import FieldModel
from models.View import ViewModel
from process.entities.EntityGet import EntityGet
from process.entities.EntityPost import EntityPost
from process.entities.ViewsPost import ViewsPost
from process.fields.FieldsPost import FieldsPost

from core_utils.functions import UUID, lambda_decorator, tenant_setup, webhook_dispatch
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
        action="can_create",
    )
    permission.check_permissions()


def _validate_payload(payload: dict) -> None:
    validate_payload(payload, ["name"])


def _get_entity_name(entity_id: str, user_id: str, tenant_id: str) -> str:
    payload = {"entity_id": entity_id}
    entity_object = EntityModel(user_id, tenant_id)
    entity_get_process = EntityGet(entity_object, payload)
    response = entity_get_process.perform()[1]
    entity_name = response["entity"].get("name")
    return entity_name


def _create_default_fields(entity_id: str, user_id: str, tenant_id: str):
    default_fields = [NAME_FIELD_PAYLOAD, IS_ACTIVE_FIELD_PAYLOAD]
    field_ids = []
    for default_field in default_fields:
        default_field["parent_object_id"] = entity_id
        field_object = FieldModel(user_id, tenant_id)
        field_post_process = FieldsPost(field_object, default_field)
        response = field_post_process.perform()
        field_ids.append(response)
    return field_ids


def _create_views(entity_id: str, user_id: str, tenant_id: str):
    entity_name = _get_entity_name(entity_id, user_id, tenant_id)
    view_types = ["list", "create", "update"]
    payload = {"entity_name": to_view_format(entity_name)}
    view_ids = []
    for view_type in view_types:
        view_object = ViewModel(user_id, tenant_id, entity_id)
        views_post_process = ViewsPost(view_object, view_type, payload)
        result, _ = views_post_process.perform()
        if result:
            view_ids.append(result)
        else:
            raise ControllerException(
                400,
                {
                    "error": f"Error creating {view_type} view.",
                    "code": "CustomEntities.ViewsPostError",
                },
            )
    return view_ids


@tenant_setup
@webhook_dispatch("entities_master", "create")
@lambda_decorator
def lambda_handler(event, context):
    user_id, tenant_id = event["user_id"], event["tenant_id"]
    payload = json.loads(event["body"])
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
    entity_post_process = EntityPost(entity_object=entity_object, payload=payload)
    response = entity_post_process.perform()
    result_fields = _create_default_fields(
        response[1].get("entity_id"), user_id, tenant_id
    )
    result_views = _create_views(response[1].get("entity_id"), user_id, tenant_id)
    if result_views and result_fields:
        response[1]["result"] = "Successful creation of the entity and views."
    return response
