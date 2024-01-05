from custom_endpoint_utils import (
    OBJECT_FIELDS_COLUMNS,
    raise_payload_error,
    validate_field_properties,
    validate_payload_field,
)
from models.CustomObject import CustomObjectModel

from core_utils.functions import UUID


class ObjectUpdate:
    def __init__(
        self, custom_object: "CustomObjectModel", payload: dict, path_params: dict, entity_id: str
    ):
        self.custom_object = custom_object
        self.payload = payload
        self.raw_entity_name = path_params.get("entity_name")
        self.entity_id = entity_id

    def perform(self):
        primary_key = self.custom_object.get_custom_object_primary_key()
        custom_fields = self.custom_object.get_custom_fields()
        single_keys, multi_keys = self._validate_payload(primary_key, custom_fields)
        self.custom_object.check_if_already_exists(self.custom_object.entity_name, self.payload)
        keys, update_data = self._setup_update_data(
            primary_key, single_keys, multi_keys, custom_fields
        )
        object_id = self.custom_object.update_object(primary_key, self.entity_id, keys, update_data)
        return (
            200,
            {
                "correlation_id": UUID,
                "result": f"{self.raw_entity_name} record update success",
                f"{primary_key}": object_id,
            },
        )

    def _validate_payload(self, primary_key: str, custom_fields: list) -> tuple:
        audit_keys = ["created_at", "created_by", "updated_at", "updated_by"]
        missing_fields = []
        object_fields = self.custom_object.get_custom_object_fields(OBJECT_FIELDS_COLUMNS)
        payload_fields = set(self.payload.keys())

        if not payload_fields:
            raise_payload_error("Payload data is empty", "CustomObjectPut.EmptyPayload")

        single_select_keys, multi_select_keys, payload_fields = self._validate_select_fields(
            custom_fields, payload_fields
        )

        missing_fields, payload_fields = self._validate_missing_fields(
            object_fields,
            primary_key,
            audit_keys,
            payload_fields,
            single_select_keys,
            multi_select_keys,
            missing_fields,
        )

        if missing_fields:
            raise_payload_error(
                f"Missing field(s) in payload data: {', '.join(missing_fields)}",
                "CustomObjectPut.MissingPayloadData",
            )

        if payload_fields:
            raise_payload_error(
                f"Invalid field(s) in payload data: {', '.join(payload_fields)}",
                "CustomObjectPut.InvalidPayloadData",
            )
        return single_select_keys, multi_select_keys

    def _validate_select_fields(self, custom_fields: list, payload_fields: dict) -> tuple:
        single_select_keys = []
        multi_select_keys = []
        for custom_field in custom_fields:
            column_name = custom_field.get("column_name")
            if column_name in payload_fields:
                is_select = custom_field.get("is_select")
                if is_select:
                    property_schemas_id = custom_field.get("property_schemas_id")
                    property_schema = self.custom_object.get_property_schema(property_schemas_id)
                    is_multiple = property_schema.get("is_multiple", False)
                    payload_fields.remove(column_name)
                    if is_multiple:
                        multi_select_keys.append(column_name)
                    else:
                        single_select_keys.append(column_name)
                else:
                    validate_field_properties(
                        column_name,
                        self.payload.get(column_name),
                        custom_field.get("input"),
                        custom_field.get("properties"),
                    )
        return single_select_keys, multi_select_keys, payload_fields

    def _validate_missing_fields(
        self,
        object_fields: list,
        primary_key: str,
        audit_keys: list,
        payload_fields: dict,
        single_select_keys: list,
        multi_select_keys: list,
        missing_fields: list,
    ) -> tuple:
        for field_data in object_fields:
            field_name = field_data["column_name"]
            if field_name == primary_key:
                payload_fields.remove(field_name)
                continue
            if field_name in audit_keys:
                if field_name in payload_fields:
                    payload_fields.remove(field_name)
                    continue
                else:
                    continue
            if field_name not in payload_fields:
                if field_name in single_select_keys:
                    continue
                if field_name in multi_select_keys:
                    continue
                missing_fields.append(field_name)
            else:
                validate_payload_field(self.payload, field_name, field_data)
                payload_fields.remove(field_name)
        return missing_fields, payload_fields

    def _setup_update_data(
        self,
        primary_key: str,
        single_select_keys: list,
        multi_select_keys: list,
        custom_fields: list,
    ) -> dict:
        predefined_keys = []
        update_data = self.payload.copy()
        select_data = ["column_name"]
        object_fields = self.custom_object.get_custom_object_fields(select_data)
        columns_to_remove = [primary_key, "created_by", "created_at"]
        object_columns = [dct["column_name"] for dct in object_fields]
        column_keys = [column for column in object_columns if column not in columns_to_remove]

        if single_select_keys:
            update_single_data = self._setup_single_select_data(single_select_keys, custom_fields)
            update_data.update(update_single_data)
        if multi_select_keys:
            update_multi_data = self._setup_multi_select_data(multi_select_keys, custom_fields)
            update_data.update(update_multi_data)

        if "updated_by" in column_keys and "updated_at" in column_keys:
            predefined_keys = ["updated_by", "updated_at"]
        if "tenant_id" in column_keys:
            predefined_keys.append("tenant_id")
            update_data["tenant_id"] = self.custom_object.tenant_id
        keys = [field for field in column_keys if field in self.payload] + predefined_keys
        return keys, update_data

    def _setup_single_select_data(self, single_select_keys: list, custom_fields: list) -> dict:
        """This method is responsible for setting up data for single-select fields."""
        payload = self.payload
        update_data = {}
        for field_key in single_select_keys:
            if not payload.get(field_key):
                update_data[field_key] = payload.get(field_key)
                continue
            fields_dict = {obj["column_name"]: obj for obj in custom_fields if "column_name" in obj}
            custom_field = fields_dict.get(f"{field_key}")

            property_schemas_id = custom_field.get("property_schemas_id")
            property_schema = self.custom_object.get_property_schema(property_schemas_id)
            is_picklist = property_schema.get("is_picklist", False)
            if is_picklist:
                update_data[field_key] = payload[field_key].get("picklist_id")
            else:
                property_entity_id = custom_field["data_source"].get("property_entity_id")
                property_entity_data = self.custom_object.get_entity_table_data(property_entity_id)
                property_entity_primary_key = property_entity_data.get(
                    "database_entity_primary_key"
                )
                update_data[field_key] = payload[field_key].get(f"{property_entity_primary_key}")
        return update_data

    def _setup_multi_select_data(self, multi_select_keys: list, custom_fields: list) -> dict:
        payload = self.payload
        update_data = {}
        for field_key in multi_select_keys:
            if not payload.get(field_key):
                update_data[field_key] = payload.get(field_key)
                continue
            fields_dict = {obj["column_name"]: obj for obj in custom_fields if "column_name" in obj}
            custom_field = fields_dict.get(f"{field_key}")
            property_schemas_id = custom_field.get("property_schemas_id")
            property_schema = self.custom_object.get_property_schema(property_schemas_id)
            is_picklist = property_schema.get("is_picklist", False)
            if is_picklist:
                picklist_multi_data = [
                    acceptable_value["picklist_id"] for acceptable_value in payload[field_key]
                ]
                update_data[field_key] = picklist_multi_data
            else:
                property_entity_id = custom_field["data_source"].get("property_entity_id")
                property_entity_data = self.custom_object.get_entity_table_data(property_entity_id)
                property_entity_primary_key = property_entity_data.get(
                    "database_entity_primary_key"
                )
                related_multi_data = [
                    acceptable_value[f"{property_entity_primary_key}"]
                    for acceptable_value in payload[field_key]
                ]
                update_data[field_key] = related_multi_data

        return update_data
