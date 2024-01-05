from entities_utils import (check_icon, raise_payload_error, to_snake_format,
                            to_view_format)
from models.Entity import EntityModel

from core_utils.functions import UUID


class EntityPut:
    def __init__(self, entity_object: "EntityModel", payload: dict, entity_id: str):
        self.entity_object = entity_object
        self.payload = payload
        self.entity_id = entity_id

    def perform(self):
        entity = self._check_entity()
        update_data = self._setup_update_data(entity)
        self.entity_object.set_fields(update_data)
        self._check_icon_data()
        response = self.entity_object.update_entity()
        return (
            200,
            {
                "correlation_id": UUID,
                "result": "Entity update successful",
                "entity_id": response.get("entity_id")
            },
        )

    def _setup_update_data(self, entity: dict) -> dict:
        entity_data = {**self.payload, **self.payload["properties"], **self.payload["icon_data"]}
        entity_data.pop("properties", None)
        entity_data.pop("icon_data", None)
        entity_data["database_name"] = to_snake_format(entity_data.get("database_name"))
        entity_data["translation_reference"] = to_snake_format(
            entity_data.get("translation_reference")
        )
        entity_data["api_name"] = to_view_format(entity_data.get("api_name"))
        entity_data["entity_id"] = self.entity_id
        return entity_data
    
    def _check_entity(self) -> dict:
        conditional = []
        conditional.append(f"entity_id = '{self.entity_id}'")
        entity = self.entity_object.get_entity_conditional(conditional=conditional)[0]
        if not entity:
            raise_payload_error(
                f"No entity exists with the following id: {self.entity_id}",
                "EntityPut.InvalidId"
            )
        return entity
    
    def _check_icon_data(self):
        if not check_icon(self.payload.get("icon_data")):
            raise_payload_error(
                "Invalid Icon data",
                "EntityPut.InvalidIconData"
            )