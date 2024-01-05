from entities_utils import check_icon, to_snake_format, to_view_format
from models.Entity import EntityModel

from core_utils.functions import UUID
from utils.exceptions.controller_exceptions import ControllerException


class EntityPost:
    def __init__(self, entity_object: "EntityModel", payload: dict):
        self.entity_object = entity_object
        self.payload = payload

    def perform(self):
        insert_data = self._setup_insert_data()
        self.entity_object.set_fields(insert_data)
        self.entity_object.check_if_entity_exists()
        self._check_icon_data()
        entity_id = self.entity_object.create_entity()
        self._create_entity_permissions()
        return (
            200,
            {
                "correlation_id": UUID,
                "result": "Create entity success",
                "entity_id": entity_id,
            },
        )

    def _create_entity_permissions(self):
        component_id = self.entity_object.create_component_master_permissions()
        self.entity_object.create_object_master(component_id)

    def _setup_insert_data(self):
        entity_data = {
            **self.payload,
            **self.payload["properties"],
            **self.payload["icon_data"],
        }
        entity_data.pop("properties", None)
        entity_data.pop("icon_data", None)
        entity_data["database_name"] = to_snake_format(entity_data.get("database_name"))
        entity_data["translation_reference"] = to_snake_format(
            entity_data.get("translation_reference")
        )
        entity_data["api_name"] = to_view_format(entity_data.get("api_name"))
        return entity_data

    def _check_icon_data(self):
        if not check_icon(self.payload.get("icon_data")):
            raise ControllerException(
                400,
                {
                    "error": "Invalid Icon data",
                    "code": "CustomEntities.InvalidIconData",
                },
            )
