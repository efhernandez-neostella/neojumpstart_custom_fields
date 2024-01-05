import json
import os
import uuid

import boto3
from jsonschema import ValidationError, validate

from utils.exceptions.controller_exceptions import ControllerException

TENANTS_BUCKET = os.environ.get("TENANTS_BUCKET")
NAME_FIELD_PAYLOAD = {
    "acceptable_values": {},
    "data_source": {},
    "column_name": "name",
    "description": "Record name default field",
    "format": None,
    "input": "text",
    "is_required": True,
    "name": "Name",
    "parent_object_id": "",
    "properties": {
        "is_required": True,
        "is_label": True,
        "label": "Name",
        "is_max_characters": True,
        "max_characters": 250,
        "is_special_characters": True,
        "default_field": True,
    },
    "height": 1,
    "width": 6,
}
IS_ACTIVE_FIELD_PAYLOAD = {
    "acceptable_values": {},
    "data_source": {},
    "column_name": "is active",
    "description": "Is active default field",
    "format": None,
    "input": "toggle",
    "is_required": True,
    "name": "Is active",
    "parent_object_id": "",
    "properties": {
        "is_default_value": True,
        "default_value": True,
        "is_label": True,
        "label": "Is Active",
        "default_field": True,
    },
    "height": 1,
    "width": 6,
}


def get_icons() -> list:
    s3_client = boto3.client("s3")
    response = s3_client.get_object(
        Bucket=TENANTS_BUCKET, Key="assets_common/icon_objects/IconObjects.json"
    )
    return json.loads(response["Body"].read())


def check_icon(icon_data: dict) -> bool:
    icons = get_icons()
    for icon in icons:
        if icon_data.get("icon_name") == icon.get("name"):
            if icon_data.get("icon_class") == icon.get("class"):
                return True
    return False


def is_uuid_valid(value) -> bool:
    if value is None or value == "":
        return False
    try:
        uuid.UUID(str(value))
    except ValueError:
        return False
    return True


def to_snake_format(input_string: str) -> str:
    formatted_string = input_string.replace(" ", "_").replace("-", "_")
    formatted_string = formatted_string.lower()
    return formatted_string


def to_view_format(input_string: str) -> str:
    formatted_string = input_string.replace(" ", "-").replace("_", "-")
    formatted_string = formatted_string.lower()
    return formatted_string


def validate_payload(payload: dict, required_fields: list) -> None:
    try:
        column_schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string", "maxLength": 50},
                "database_name": {"type": "string", "maxLength": 50},
                "translation_reference": {"type": "string", "maxLength": 50},
                "api_name": {"type": "string", "maxLength": 50},
                "properties": {
                    "can_edit_fields": {"type": "boolean"},
                    "can_edit_views": {"type": "boolean"},
                    "can_remove": {"type": "boolean"},
                    "can_soft_delete": {"type": "boolean"},
                    "has_file_storage": {"type": "boolean"},
                },
                "icon_data": {"icon_name": {"type": "string"}, "icon_class": {"type": "string"}},
            },
            "required": required_fields,
        }
        database_name = payload.get("database_name")
        if not (isinstance(database_name, str) and " " not in database_name):
            raise ControllerException(
                400,
                {
                    "error": "Invalid database_name. It should be a string without spaces.",
                    "code": "CustomEntities.InvalidBody",
                },
            )

        for field_name, field_value in payload.items():
            if field_value == "":
                raise ControllerException(
                    400,
                    {
                        "error": f"{field_name} cannot be an empty string",
                        "code": "CustomEntities.InvalidBody",
                    },
                )
        validate(instance=payload, schema=column_schema)
    except ValidationError as e:
        raise ControllerException(400, {"error": e.message, "code": "CustomEntities.InvalidBody"})


def raise_payload_error(error_message: str, error_code: str):
    raise ControllerException(
        400,
        {
            "error": error_message,
            "code": error_code,
        },
    )
