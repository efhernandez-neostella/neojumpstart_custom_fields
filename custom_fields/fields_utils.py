from decimal import Decimal

from jsonschema import ValidationError, validate

from core_utils.functions import UUID
from utils.exceptions.controller_exceptions import ControllerException


def to_snake_format(input_string: str) -> str:
    formatted_string = input_string.replace(" ", "_").replace("-", "_")
    formatted_string = formatted_string.lower()
    return formatted_string


def dynamo_decimal_parse(items: dict) -> dict:
    for key, value in items.items():
        if isinstance(value, float):
            items[key] = Decimal(str(value))
    return items


def validate_payload(payload: dict, required_fields: list) -> None:
    try:
        column_schema = {
            "type": "object",
            "properties": {
                "acceptable_values": {
                    "type": ["array", "object", "null"],
                    "items": {
                        "type": ["object", "null"],
                        "properties": {
                            "name": {"type": "string"},
                            "is_active": {"type": "boolean"},
                            "order": {"type": "integer"},
                        },
                        "required": ["name", "is_active", "order"],
                        "additionalProperties": True,
                    },
                },
                "data_source": {"type": "object"},
                "column_name": {"type": "string"},
                "data_type": {"type": "string"},
                "description": {"type": "string"},
                "format": {"type": ["string", "null"]},
                "input": {"type": "string"},
                "is_required": {"type": "boolean"},
                "name": {"type": "string"},
                "parent_object_id": {"type": "string"},
                "properties": {"type": "object"},
            },
            "required": required_fields,
        }
        validate(instance=payload, schema=column_schema)
    except ValidationError as e:
        raise ControllerException(400, {"error": e.message, "code": "CustomFields.InvalidBody"})


def raise_payload_error(error_message: str, error_code: str):
    raise ControllerException(
        400,
        {
            "correlation_id": UUID,
            "error": error_message,
            "code": error_code,
        },
    )
