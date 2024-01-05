from controllers.views.schemas.Views import JsonContent, ViewSerializer
from views_utils import (
    VIEWS,
    check_field_entity,
    get_view_entity_id,
    validate_json_content,
    validate_view_id_exists,
)

from core_utils.functions import UUID


class ViewsPut:
    def __init__(self, view_object: ViewSerializer, json_content: JsonContent = None):
        self.view_object = view_object
        self.json_content = json_content

    def _update_view_conf(self) -> None:
        """Function that updates the dynamo record that have the view configuration"""

        # If not sent, don't update the dynamo record.
        if self.json_content is None:
            return

        entity_id = get_view_entity_id(self.view_object.view_id)
        json_conf = self.json_content.dict()
        validate_json_content(json_conf)
        cards = json_conf.get("cards", [])
        # Remove fields info that is stored in the fields table
        for card in cards:
            sections = card.get("sections", [])
            for section in sections:
                fields = section.get("fields", [])
                if fields:
                    section["fields"] = [
                        {"custom_field_id": field["custom_field_id"]}
                        for field in fields
                        if check_field_entity(entity_id, field.get("custom_field_id", None))
                    ]
        # Create the dynamo record structure
        dynamo_obj = {
            "view_id": self.view_object.view_id,
            "entity_id": entity_id,
            "tenant_id": self.view_object.tenant_id,
            "json_content": json_conf,
        }
        VIEWS.put_item(Item=dynamo_obj)

    def perform(self) -> tuple:
        validate_view_id_exists(self.view_object.view_id)
        # Update record in SQL and in dynamo
        self.view_object.put_views()
        self._update_view_conf()
        return (
            200,
            {
                "correlation_id": UUID,
                "view_id": self.view_object.view_id,
                "result": "Updated view",
            },
        )
