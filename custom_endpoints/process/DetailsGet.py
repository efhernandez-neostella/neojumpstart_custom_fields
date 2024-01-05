from custom_endpoint_utils import raise_payload_error
from models.CustomObject import CustomObjectModel

from core_utils.functions import UUID


class ObjectDetails:
    def __init__(
        self, custom_object: "CustomObjectModel", path_params: dict, entity_id: str
    ):
        self.custom_object = custom_object
        self.raw_entity_name = path_params.get("entity_name")
        self.entity_id = entity_id

    def perform(self):
        conditional = self._set_conditional()
        object = self.custom_object.get_custom_object(conditional)
        custom_fields = self.custom_object.get_custom_fields()

        filtered_custom_fields = [
            obj for obj in custom_fields if obj.get("is_select", False)
        ]
        parsed_object = self._set_field_data_structure(filtered_custom_fields, object)

        if parsed_object:
            return (
                200,
                {
                    "correlation_id": UUID,
                    "result": f"Get {self.raw_entity_name} record success",
                    "record": parsed_object,
                },
            )
        else:
            raise_payload_error(
                f"No record exists for {self.raw_entity_name} \
                    with the following id: {self.entity_id}",
                "CustomObject.InvalidRecordIdError",
            )

    def _set_field_data_structure(
        self, filtered_custom_fields: list, object: dict
    ) -> dict:
        """
        Set field data structure in the provided object based on filtered custom fields and if it is
        picklist, is multiple or is single select.

        Args:
            filtered_custom_fields (list): List of custom fields to be processed.
            object (dict): The object to which the field data will be added.

        Returns:
            dict: The modified object with updated field data with the matching pick list object or
            multiselect object or single select object.
        """
        property_schemas_ids = [
            custom_field["property_schemas_id"]
            for custom_field in filtered_custom_fields
        ]
        values = self.custom_object.get_values(property_schemas_ids)
        if not filtered_custom_fields:
            return object
        for custom_field in filtered_custom_fields:
            property_schemas_id = custom_field["property_schemas_id"]
            filtered_values = next(
                (
                    obj
                    for obj in values
                    if obj.get("property_schemas_id") == property_schemas_id
                ),
                None,
            )

            if not filtered_values:
                continue

            if filtered_values["is_picklist"]:
                column_name = custom_field["column_name"]
                picklist_ids = object[column_name]
                if isinstance(picklist_ids, str):
                    picklist_ids = [picklist_ids]
                if not picklist_ids:
                    continue
                picklist_data = self.custom_object.get_picklist_data(picklist_ids)
                object[column_name] = (
                    picklist_data
                    if filtered_values["is_multiple"] == True
                    else picklist_data[0]
                )
                continue

            column_name = custom_field["column_name"]
            arr_objects = object[column_name]

            if not arr_objects:
                continue

            if isinstance(arr_objects, str):
                arr_objects = [arr_objects]

            fields_data = self.custom_object.get_field_data(
                filtered_values["database_name"],
                arr_objects,
                filtered_values["custom_field_name"],
                filtered_values["column_name_pk"],
            )

            object[column_name] = (
                fields_data
                if filtered_values["is_multiple"] == True
                else fields_data[0]
            )

        return object

    def _set_conditional(self) -> list:
        conditional = []
        primary_key = self.custom_object.get_custom_object_primary_key()
        conditional.append(f"{primary_key} = '{self.entity_id}'")
        return conditional
