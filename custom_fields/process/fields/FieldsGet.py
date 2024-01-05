from models.Field import FieldModel

from core_utils.functions import UUID


class FieldsGet:
    def __init__(self, field_object: "FieldModel"):
        self.field_object = field_object

    def perform(self):
        fields = self.field_object.get_fields()
        for field in fields:
            acceptable_values = self._process_field(field)
            field.update({"acceptable_values": acceptable_values})
        return (200, {"correlation_id": UUID, "result": "Get fields success", "fields": fields})

    def _process_field(self, field: dict) -> list:
        custom_field_id = field.get("custom_field_id")
        properties = field.get("properties", {})
        is_picklist = properties.get("is_picklist", False)

        if is_picklist:
            custom_field_id = field.get("custom_field_id")
            return self.field_object.get_picklist_values(custom_field_id)
        else:
            return []
