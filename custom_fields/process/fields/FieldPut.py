from fields_utils import raise_payload_error
from models.Field import FieldModel


class FieldPut:
    def __init__(self, field_object: "FieldModel", payload: dict, field_id: str):
        self.field_object = field_object
        self.payload = payload
        self.field_id = field_id

    def perform(self):
        keys = self._setup_update_data()
        self.field_object.parent_object_exist()
        field_exist = self.field_object.get_custom_field(self.field_id)
        if not field_exist:
            raise_payload_error(
                f"There is not a field with the id {self.field_id}", "CustomFields.DoesNotExists"
            )
        if self._check_properties():
            self.update_acceptable_values()
        result = self.field_object.update_field_record(keys)
        return (
            200,
            {
                "result": "Field update success",
                "custom_field_id": result.get("custom_field_id"),
            },
        )

    def _setup_update_data(self) -> list:
        self.field_object.set_fields(self.payload)
        data_type = self.field_object.get_field_type()
        self.field_object.fields["data_type"] = data_type
        self.field_object.fields["custom_field_id"] = self.field_id
        keys = ["is_active", "description", "updated_by", "updated_at"]
        return keys

    def _check_properties(self) -> bool:
        payload = self.payload
        input_type = payload.get("input")

        field_input_types = ["autocomplete", "checkbox", "radio"]

        if input_type not in field_input_types:
            return False

        properties = self.payload.get("properties", {})
        is_picklist = properties.get("is_picklist", False)

        if is_picklist:
            self._validate_picklist()
            return True

        return False

    def _validate_picklist(self):
        acceptable_values = self.payload.get("acceptable_values", [])
        self._validate_non_empty_acceptable_values(acceptable_values)
        self._validate_maximum_acceptable_values(acceptable_values)
        self._validate_picklist_names(acceptable_values)

    def _validate_non_empty_acceptable_values(self, acceptable_values: list):
        if not acceptable_values:
            raise_payload_error(
                "There are no acceptable values for the field with picklist property.",
                "CustomFields.EmptyAcceptableValues",
            )

    def _validate_maximum_acceptable_values(self, acceptable_values: list):
        if len(acceptable_values) > 20:
            raise_payload_error(
                "There are more than 20 acceptable values.",
                "CustomFields.MaximumAcceptableValues",
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

    def update_acceptable_values(self) -> None:
        acceptable_values = self.field_object.fields.get("acceptable_values", [])
        custom_field_id = self.field_object.fields.get("custom_field_id")
        picklist_values = self.field_object.get_picklist_values(custom_field_id)
        picklist_ids = {picklist_object["picklist_id"] for picklist_object in picklist_values}

        for picklist_data in acceptable_values:
            picklist_id = picklist_data.get("picklist_id", "")
            picklist_data.update({"order_num": picklist_data.get("order")})
            if picklist_id:
                if picklist_id in picklist_ids:
                    self.field_object.update_picklist_values(picklist_id, picklist_data)
                else:
                    raise_payload_error(
                        f"There is no record with the following picklist_id: {picklist_id}",
                        "CustomFields.InvalidPicklistId",
                    )
            else:
                self.field_object.insert_picklist_values(picklist_data)
