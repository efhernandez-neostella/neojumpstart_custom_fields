from models.CustomObject import CustomObjectModel

from core_utils.functions import UUID


class ObjectList:
    def __init__(self, custom_object: "CustomObjectModel", parameters: dict, path_params: dict):
        self.custom_object = custom_object
        self.parameters = parameters
        self.raw_entity_name = path_params.get("entity_name")

    def perform(self):
        conditionals = self._set_conditionals()
        objects = self.custom_object.get_custom_objects(conditionals)
        parsed_objects = [self._parse_object(object) for object in objects]
        custom_fields = self.custom_object.get_custom_fields()

        filtered_custom_fields = [obj for obj in custom_fields if obj.get("is_select", False)]
        property_schemas_ids = [
            custom_field["property_schemas_id"] for custom_field in filtered_custom_fields
        ]
        values = self.custom_object.get_values(property_schemas_ids)
        for object in parsed_objects:
            self.process_custom_fields(filtered_custom_fields, object, values)
        return (
            200,
            {
                "correlation_id": UUID,
                "result": f"Get {self.raw_entity_name} success",
                "count": len(parsed_objects),
                "records": parsed_objects,
            },
        )

    def process_custom_fields(
        self, filtered_custom_fields: list, object: dict, values: list
    ) -> None:
        for custom_field in filtered_custom_fields:
            property_schemas_id = custom_field["property_schemas_id"]
            filtered_values = self.find_filtered_values(values, property_schemas_id)

            if not filtered_values:
                continue

            if filtered_values["is_multiple"]:
                self.remove_column(object, custom_field["column_name"])
                continue

            if filtered_values["is_picklist"]:
                self.process_picklist(object, custom_field)
                continue

            self.process_regular_field(object, custom_field, filtered_values)

    def find_filtered_values(self, values: list, property_schemas_id: str) -> dict:
        return next(
            (obj for obj in values if obj.get("property_schemas_id") == property_schemas_id),
            None,
        )

    def remove_column(self, obj: dict, column_name: str) -> None:
        obj.pop(column_name)

    def process_picklist(self, obj, custom_field):
        column_name = custom_field["column_name"]
        picklist_ids = obj[column_name]

        if isinstance(picklist_ids, str):
            picklist_ids = [picklist_ids]

        if not picklist_ids:
            return

        picklist_data = self.custom_object.get_picklist_data(picklist_ids)
        obj[column_name] = picklist_data if len(picklist_data) > 1 else picklist_data[0]["name"]

    def process_regular_field(self, obj: dict, custom_field: dict, filtered_values: dict) -> None:
        column_name = custom_field["column_name"]
        arr_objects = obj[column_name]

        if not arr_objects:
            return

        if isinstance(arr_objects, str):
            arr_objects = [arr_objects]

        fields_data = self.custom_object.get_field_data(
            filtered_values["database_name"],
            arr_objects,
            filtered_values["custom_field_name"],
            filtered_values["column_name_pk"],
        )

        obj[column_name] = (
            fields_data
            if len(fields_data) > 1
            else fields_data[0][filtered_values["custom_field_name"]]
        )

    def _set_conditionals(self) -> list:
        conditionals = []
        for key, value in self.parameters.items():
            if key == "is_active":
                conditionals.append(f"is_active = {str(value)}")
            elif key == "tenant_id":
                conditionals.append(f"tenant_id = '{value}'")
            elif key == "cognito_user_id":
                conditionals.append(f"cognito_user_id = '{value}'")

        return conditionals

    def _parse_object(self, object: dict) -> dict:
        object.update(
            {
                "created_by": object.pop("crated_by_full_name", None),
                "updated_by": object.pop("updated_by_full_name", None),
            }
        )

        keys_to_remove = ["created_by_id", "updated_by_id"]
        object = {key: value for key, value in object.items() if key not in keys_to_remove}

        return object
