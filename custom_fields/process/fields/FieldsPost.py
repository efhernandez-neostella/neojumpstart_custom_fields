from fields_utils import raise_payload_error, to_snake_format
from models.Field import FieldModel

from core_utils.functions import UUID


class FieldsPost:
    def __init__(self, field_object: "FieldModel", payload: dict):
        self.field_object = field_object
        self.payload = payload

    def perform(self):
        keys = self._setup_insert_data()
        parent_object_name = self.field_object.parent_object_exist()
        self.field_object.format_field_column_name()
        self.field_object.check_if_field_exist()
        self._validate_data_source()
        self.field_object.create_field_record()
        if self._check_properties(parent_object_name):
            self.field_object.add_field_to_object(parent_object_name)
        self.field_object.update_field_record(keys)
        return (
            200,
            {
                "correlation_id": UUID,
                "result": "Field creation success",
                "custom_field_id": self.field_object.fields["custom_field_id"],
            },
        )

    def _validate_data_source(self):
        payload = self.payload
        input_type = payload.get("input")
        field_input_types = ["autocomplete", "checkbox", "radio"]
        properties = payload.get("properties")

        if input_type not in field_input_types:
            return

        if properties.get("is_picklist") is True:
            self._validate_picklist()
            return

        data_source = payload.get("data_source", {})
        property_entity_id = data_source.get("property_entity_id")
        parent_object_id = payload.get("parent_object_id")

        if property_entity_id == parent_object_id:
            raise_payload_error(
                "The reference entity cannot be the same as the entity of the field being created.",
                "CustomFields.InvalidAcceptableValues",
            )

        property_field_id = data_source.get("property_field_id")
        custom_field = self.field_object.get_custom_field(property_field_id)[0]

        if custom_field.get("input") in [
            "autocomplete",
            "date",
            "toggle",
            "date_time",
            "email",
            "url",
        ]:
            raise_payload_error(
                f"The reference custom field type field cannot be {custom_field.get('input')}",
                "CustomFields.InvalidAcceptableValues",
            )

    def _validate_picklist(self):
        acceptable_values = self.payload.get("acceptable_values", [])
        self._validate_non_empty_acceptable_values(acceptable_values)
        self._validate_picklist_names(acceptable_values)

    def _validate_non_empty_acceptable_values(self, acceptable_values: list):
        if not acceptable_values:
            raise_payload_error(
                "There are no acceptable values for the field with picklist property.",
                "CustomFields.EmptyAcceptableValues",
            )

    def _validate_picklist_names(self, acceptable_values: list):
        name_set = set()
        for obj in acceptable_values:
            if "name" in obj:
                name = obj["name"]
                if name in name_set:
                    raise_payload_error(
                        "There are duplicate acceptable values in the picklist.",
                        "CustomFields.DuplicateAcceptableValues",
                    )
                name_set.add(name)

    def _setup_insert_data(self) -> list:
        self.payload["column_name"] = to_snake_format(self.payload.get("column_name"))
        self.field_object.set_fields(self.payload)
        data_type = self.field_object.get_field_type()
        self.field_object.fields["data_type"] = data_type
        keys = ["data_type", "is_select", "property_schemas_id"]
        return keys

    def _check_properties(self, parent_object_name) -> bool:
        payload = self.payload
        input_type = payload.get("input")

        field_input_types = ["autocomplete", "checkbox", "radio"]

        if input_type not in field_input_types:
            return True

        properties = self.payload.get("properties", {})
        is_picklist = properties.get("is_picklist", False)
        is_multiple = properties.get("is_multiple", False)

        if input_type == "autocomplete":
            self._handle_select(True, is_multiple, is_picklist, parent_object_name)
        elif input_type == "checkbox":
            self._handle_select(True, True, is_picklist, parent_object_name)
        elif input_type == "radio":
            self._handle_select(True, False, is_picklist, parent_object_name)

        return False

    def _handle_select(
        self, is_select: bool, is_multiple: bool, is_picklist: bool, parent_object_name=None
    ):
        self.field_object.fields["is_select"] = is_select

        insert_data = self._get_insert_data(is_picklist, is_multiple)

        property_schemas_id = self.field_object.create_property_schema(insert_data)
        self.field_object.fields["property_schemas_id"] = property_schemas_id

        if is_multiple:
            self.field_object.add_field_to_object(parent_object_name)
        else:
            self._handle_single_select(parent_object_name, is_picklist)

    def _get_insert_data(self, is_picklist: bool, is_multiple: bool):
        payload = self.payload
        insert_data = payload.get("data_source", {}).copy()

        if is_picklist:
            self._handle_picklist_values(payload, insert_data, is_picklist, is_multiple)
        else:
            insert_data.update({"is_picklist": is_picklist, "is_multiple": is_multiple})

        return insert_data

    def _handle_picklist_values(self, payload, insert_data, is_picklist, is_multiple):
        picklist_objects = payload.get("acceptable_values", [])
        for obj in picklist_objects:
            obj.update({"order_num": obj.get("order")})
            self.field_object.insert_picklist_values(obj)

        insert_data.update(
            {
                "property_entity_id": None,
                "property_field_id": None,
                "is_picklist": is_picklist,
                "is_multiple": is_multiple,
            }
        )

    def _handle_single_select(self, parent_object_name, is_picklist):
        payload = self.payload
        data_source = payload.get("data_source", {})
        property_entity_id = data_source.get("property_entity_id")
        self.field_object.fields["data_type"] = "uuid"

        if parent_object_name:
            if is_picklist:
                entity_data = {
                    "database_name": "picklist_master",
                    "database_entity_primary_key": "picklist_id",
                }
            else:
                entity_data = self.field_object.get_entity_linking_table_data(property_entity_id)
            self.field_object.add_foreign_field_to_object(parent_object_name, entity_data)
