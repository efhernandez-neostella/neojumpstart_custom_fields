from entities_utils import to_view_format
from models.Entity import EntityModel

from core_utils.functions import UUID
from utils.common import handle_payload_error


class EntitiesViewsGet:
    def __init__(self, entity_object: "EntityModel", payload: dict):
        self.entity_object = entity_object
        self.payload = payload

    def perform(self):
        valid, code = self.entity_object.entities_get_validate_payload(self.payload)
        handle_payload_error(valid, code)
        entities_views = self.entity_object.get_entities_views()
        parsed_entities_views = [self._parse_entity(entity) for entity in entities_views]
        return (
            200,
            {
                "correlation_id": UUID,
                "result": "Get entities success",
                "entities": parsed_entities_views
            },
        )

    def _parse_entity(self, entity: dict) -> dict:
        views = (entity.get("views") or [])
        api_name = to_view_format(entity.get("api_name"))
        sorted_views = sorted(views, key=lambda view: self._view_type_key(api_name, view))
        return {
            "entity_id": entity.get("entity_id"),
            "name": entity.get("name"),
            "in_drawer": entity.get("in_drawer"),
            "icon_name": entity.get("icon_name") or "",
            "views": sorted_views,
            "database_name": entity.get("database_name")
        }

    def _view_type_key(self, api_name: str, view: dict) -> int:
        route = view.get("view_route", "")

        if route.endswith(f"/{api_name}"):
            return 1
        elif route.endswith(f"/{api_name}/create"):
            return 2
        elif route.endswith(f"/{api_name}/{{id}}"):
            return 3
        else:
            return 4
