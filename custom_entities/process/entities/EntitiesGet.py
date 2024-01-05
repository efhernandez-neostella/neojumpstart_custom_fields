from models.Entity import EntityModel

from core_utils.functions import UUID
from utils.common import handle_payload_error


class EntitiesGet:
    def __init__(self, entity_object: "EntityModel", payload: dict):
        self.entity_object = entity_object
        self.payload = payload

    def perform(self):
        valid, code = self.entity_object.entities_get_validate_payload(self.payload)  # noqa: E501
        handle_payload_error(valid, code)
        entities = self.entity_object.get_entities()
        parsed_entities = [self._parse_entity(entity) for entity in entities]
        return (
            200,
            {
                "correlation_id": UUID,
                "result": "Get entities success",
                "entities": parsed_entities,
            },  # noqa: E501
        )

    def _parse_entity(self, entity) -> dict:
        return {
            "entity_id": entity.get("entity_id"),
            "name": entity.get("name"),
            "is_active": entity.get("is_active"),
            "database_name": entity.get("database_name"),
            "translation_reference": entity.get("translation_reference"),
            "api_name": entity.get("api_name"),
            "can_edit_fields": entity.get("can_edit_fields"),
            "can_edit_views": entity.get("can_edit_views"),
            "can_remove": entity.get("can_remove"),
            "can_soft_delete": entity.get("can_soft_delete"),
            "has_file_storage": entity.get("has_file_storage"),
            "icon_data": {
                "icon_name": entity.get("icon_name") or "",
                "icon_class": entity.get("icon_class") or ""
            },
            "audit_data": entity.get("audit_data"),
        }
