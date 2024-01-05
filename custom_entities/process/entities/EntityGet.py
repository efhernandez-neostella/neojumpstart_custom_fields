from entities_utils import to_view_format
from models.Entity import EntityModel
from models.Field import FieldModel

from core_utils.functions import UUID
from utils.common import handle_payload_error


class EntityGet:
    def __init__(self, entity_object: "EntityModel", payload: dict):
        self.entity_object = entity_object
        self.payload = payload

    def perform(self):
        valid, code = self.entity_object.entity_get_validate_payload(self.payload)
        handle_payload_error(valid, code)
        entity = self.entity_object.get_entity()
        parsed_entity = self._parse_entity(entity)
        return (
            200,
            {"correlation_id": UUID, "result": "Get entity success", "entity": parsed_entity},
        )

    def _parse_entity(self, entity) -> dict:
        return {
            "entity_id": entity.get("entity_id"),
            "name": entity.get("name"),
            "is_active": entity.get("is_active"),
            "database_name": entity.get("database_name"),
            "entity_table_primary_key": entity.get("database_entity_primary_key"),
            "translation_reference": entity.get("translation_reference"),
            "api_name": entity.get("api_name"),
            "can_edit_fields": entity.get("can_edit_fields"),
            "can_edit_views": entity.get("can_edit_views"),
            "can_remove": entity.get("can_remove"),
            "can_soft_delete": entity.get("can_soft_delete"),
            "has_file_storage": entity.get("has_file_storage"),
            "is_base": entity.get("is_base"),
            "component_id": entity.get("component_id"),
            "linking_table": entity.get("linking_table"),
            "icon_data": {
                "icon_name": entity.get("icon_name") or "",
                "icon_class": entity.get("icon_class") or "",
            },
            "views": [
                self._parse_view(entity["api_name"], view) for view in (entity.get("views") or [])
            ],
            "fields": [
                self._parse_field_properties(field) for field in (entity.get("fields") or [])
            ],
            "columns": entity.get("columns") or [],
            "audit_data": entity.get("audit_data"),
        }

    def _parse_view(self, api_name: str, view: dict) -> dict:
        entity_name = to_view_format(api_name)
        route = view.get("view_route", "")

        if route.endswith(f"/{entity_name}"):
            endpoint_type = "LIST"
        elif route.endswith(f"/{entity_name}/{{id}}"):
            endpoint_type = "DETAILS/UPDATE"
        elif route.endswith(f"/{entity_name}/create"):
            endpoint_type = "CREATE"
        else:
            endpoint_type = ""

        view["endpoint_type"] = endpoint_type
        return view

    def _parse_field_properties(self, field: dict) -> dict:
        custom_field_id = field.get("custom_field_id", "")
        field_properties = {}
        if custom_field_id:
            field_properties = FieldModel.get_field_properties(
                custom_field_id, self.entity_object.tenant_id
            )
        field["properties"] = field_properties.get("properties")
        return field
