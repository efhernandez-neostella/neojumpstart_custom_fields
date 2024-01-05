from models.Field import FieldModel

from utils.common import to_snake_format


class FieldsPost:
    def __init__(self, field_object: "FieldModel", payload: dict):
        self.field_object = field_object
        self.payload = payload

    def perform(self):
        self._setup_insert_data()
        parent_object_name = self.field_object.parent_object_exist()
        self.field_object.format_field_column_name()
        self.field_object.check_if_field_exist()
        self.field_object.create_field_record()
        self.field_object.add_field_to_object(parent_object_name=parent_object_name)
        return {"custom_field_id": self.field_object.fields["custom_field_id"]}
 
    def _setup_insert_data(self):
        self.payload["column_name"] = to_snake_format(self.payload.get("column_name"))
        self.field_object.set_fields(self.payload)
        data_type = self.field_object.get_field_type()
        self.field_object.fields["data_type"] = data_type
