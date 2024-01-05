import uuid

from core_utils.functions import UUID
from sql_handler.sql_tables_handler import SQLTable, execute
from utils.exceptions.controller_exceptions import ControllerException

OBJECT_FIELDS_COLUMNS = [
    "column_name",
    "column_default",
    "is_nullable",
    "data_type",
    "character_maximum_length",
]


def extract_path(path: str) -> dict:
    """
    The possible paths that should enter the custom endpoint service controllers are:
    /custom/entity_name/{id}/... or /specific/entity_name/{id}/
    """
    parts = path.split("/")
    if len(parts) >= 3:
        if parts[1] in ["specific", "custom"]:
            entity_name_resource = parts[2]
            if len(parts) >= 4:
                id_resource = parts[3]
                return {"entity_name": entity_name_resource, "id": id_resource}
            return {"entity_name": entity_name_resource}
    return {}


def get_entity_name(parameters: dict, lambda_name: str) -> str:
    if raw_entity_name := parameters.get("entity_name"):
        entity_info = get_entity_table_name(to_snake_format(raw_entity_name))
        entity_name = entity_info.get("database_name")
        entity_id = entity_info.get("entity_id")
        if is_valid_entity_name_format(entity_name):
            return entity_id, entity_name
    raise ControllerException(
        400, {"correlation_id": UUID, "code": f"{lambda_name}.InvalidEntityName"}
    )


def get_entity_id(parameters: dict, lambda_name: str) -> str:
    if entity_id := parameters.get("id"):
        if is_uuid_valid(entity_id):
            return entity_id
    raise ControllerException(
        400, {"correlation_id": UUID, "code": f"{lambda_name}.InvalidEntityId"}
    )


def is_uuid_valid(value) -> bool:
    if value is None or value == "":
        return False
    try:
        uuid.UUID(str(value))
    except ValueError:
        return False
    return True


def eval_bool(value: str) -> bool:
    true_options = ["True", "true", "1"]
    false_options = ["False", "false", "0"]
    if value in true_options:
        return True
    elif value in false_options:
        return False


def to_snake_format(input_string: str) -> str:
    formatted_string = input_string.replace(" ", "_").replace("-", "_")
    formatted_string = formatted_string.lower()
    return formatted_string


def is_valid_entity_name_format(string: str) -> bool:
    parts = string.split("_")
    for part in parts:
        if not part.isalnum():
            return False
    return True


def get_entity_unique_constraint(entity_name_table: str) -> list:
    sql = SQLTable()
    table = "pg_catalog.pg_constraint cons"
    select_query = [
        "conname AS constraint_name",
        "contype AS constraint_type",
        "a.attname AS column_name",
    ]
    join_query_1 = "pg_catalog.pg_class t ON t.oid = cons.conrelid"
    join_query_2 = "pg_catalog.pg_attribute a ON a.attnum = ANY(cons.conkey) \
        AND a.attrelid = cons.conrelid"
    where_query = [f"t.relname = '{entity_name_table}'", "contype = 'u'"]
    query = (
        sql.select(table, select_query)
        .inner_join(join_query_1)
        .inner_join(join_query_2)
        .where(where_query)
        .query
    )
    return execute(query, table, False)


def get_entity_table_name(raw_entity_name: str) -> str:
    sql = SQLTable()
    table = "entities_master "
    select_query = ["entity_id", "database_name"]
    where_query = [f"name ILIKE '%{raw_entity_name}%'"]
    query = sql.select(table, select_query).where(where_query).query
    return execute(query)[0]


def get_table_primary_key(table_name: str):
    sql = SQLTable()
    select_query = ["c.column_name AS database_primary_key"]
    table = "information_schema.key_column_usage AS c"
    left_join_query = "information_schema.table_constraints \
        AS t ON t.constraint_name = c.constraint_name"
    where_query = [f"t.table_name = '{table_name}'", "t.constraint_type = 'PRIMARY KEY'"]
    query = sql.select(table, select_query).inner_join(left_join_query).where(where_query).query
    return execute(query)[0]["database_primary_key"]


def check_entity(input_entity):
    non_valid_entities = [
        "users_master",
        "views_master",
        "custom_fields_master",
        "entities_master",
        "view_elements",
    ]
    if input_entity in non_valid_entities:
        raise ControllerException(
            400,
            {
                "error": f"Non-valid entity ({input_entity}) error",
                "code": "CustomObject.NonValidEntityError",
            },
        )


def validate_payload_field(payload, field, field_data):
    data_type = field_data["data_type"]
    value = payload[field]

    if field_data["is_nullable"] == "NO" and (value is None or value == ""):
        raise_payload_error(f"Field '{field}' is required", "CustomObject.FieldValidationError")

    if value is not None and data_type == "character varying" and not isinstance(value, str):
        raise_payload_error(
            f"Field '{field}' must be a string", "CustomObject.FieldValidationError"
        )

    if (
        value is not None
        and data_type == "uuid"
        and (not isinstance(value, str) or not is_uuid_valid(value))
    ):
        raise_payload_error(
            f"Field '{field}' must be a valid UUID", "CustomObject.FieldValidationError"
        )

    if value is not None and data_type == "integer" and not isinstance(value, (int, float)):
        raise_payload_error(
            f"Field '{field}' must be a number", "CustomObject.FieldValidationError"
        )

    if value is not None and data_type == "boolean" and not isinstance(value, bool):
        raise_payload_error(
            f"Field '{field}' must be a boolean", "CustomObject.FieldValidationError"
        )

    if value is not None and data_type == "timestamp without time zone":
        if not isinstance(value, str):
            raise_payload_error(
                f"Field '{field}' must be a string", "CustomObject.FieldValidationError"
            )

    max_length = field_data["character_maximum_length"]
    if max_length is not None and len(str(value)) > max_length:
        raise_payload_error(
            f"Field '{field}' exceeds maximum length", "CustomObject.FieldValidationError"
        )


def validate_field_properties(key: str, value: str, field_type: str, properties: dict):
    for prop, prop_value in properties.items():
        if prop == "is_required" and prop_value:
            if not value:
                raise_payload_error(
                    f"The field: {key} is required.", "CustomObject.FieldValidationError"
                )

        elif prop == "max_characters" and prop_value:
            max_characters = prop_value
            if len(value) > float(max_characters):
                raise_payload_error(
                    f"Maximum characters allowed for field {key} is: {max_characters}",
                    "CustomObject.FieldValidationError",
                )

        elif prop == "is_special_characters" and not prop_value:
            if value is not None and not value.isalnum():
                raise_payload_error(
                    f"Special characters are not allowed for field '{key}'",
                    "CustomObject.FieldValidationError",
                )

        elif prop == "is_decimal" and prop_value:
            decimals = properties.get("decimal", None)
            if not isinstance(value, float):
                raise_payload_error(
                    f"Field {key} must be a decimal number.",
                    "CustomObject.FieldValidationError",
                )
            if decimals is not None:
                decimal_part = str(value).split(".")[1] if "." in str(value) else ""
                if len(decimal_part) < decimals:
                    raise_payload_error(
                        f"Field {key} must have exactly {decimals} decimal places.",
                        "CustomObject.FieldValidationError",
                    )
        elif prop == "is_allow_negative_numbers" and not prop_value:
            if value is not None and float(value) < 0:
                raise_payload_error(
                    f"Field {key} can not be negative number",
                    "CustomObject.FieldValidationError",
                )

        elif prop == "is_max_value" and prop_value:
            max_value = properties.get("max_value")
            if value is not None:
                float_value = float(value)
                if float_value > max_value:
                    raise_payload_error(
                        f"Field {key} must be less than or equal to {max_value}.",
                        "CustomObject.FieldValidationError",
                    )

        elif prop == "is_min_value" and prop_value:
            min_value = properties.get("min_value")
            if value is not None:
                float_value = float(value)
                if float_value < min_value:
                    raise_payload_error(
                        f"Field {key} must be greater than or equal to {min_value}.",
                        "CustomObject.FieldValidationError",
                    )


def raise_payload_error(error_message: str, error_code: str):
    raise ControllerException(
        400,
        {
            "error": error_message,
            "code": error_code,
        },
    )
