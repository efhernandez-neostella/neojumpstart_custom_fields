import json
import os
import uuid

import boto3

from core_utils.functions import (
    REGION_NAME,
    deserialize_rds_response,
    rds_execute_statement,
)
from sql_handler.sql_tables_handler import SQLTable
from utils.exceptions.controller_exceptions import ControllerException

VIEWS_TABLE = os.getenv("VIEWS_TABLE")
FIELDS_TABLE = os.getenv("FIELDS_TABLE")
DYNAMO_RESOURCE = boto3.resource("dynamodb", region_name=REGION_NAME)
DYNAMO_CLIENT = boto3.client("dynamodb", region_name=REGION_NAME)
FIELDS = DYNAMO_RESOURCE.Table(FIELDS_TABLE)
VIEWS = DYNAMO_RESOURCE.Table(VIEWS_TABLE)


def is_uuid_valid(value) -> bool:
    if value is None or value == "":
        return False
    try:
        uuid.UUID(str(value))
    except ValueError:
        return False
    return True


def validate_view_id_exists(view_id: str) -> bool:
    """Function to validate the view exists in the DB

    Args:
        view_id (str): View UUID

    Raises:
        ControllerException: raise if the view doesn't exist in the DB

    Returns:
        bool: True if the view exist in the DB
    """
    sql = SQLTable().select("views_master", ["COUNT(*)"]).where([f"view_id='{view_id}'"]).query
    count = deserialize_rds_response(rds_execute_statement(sql))[0]["count"]

    if not count:
        raise ControllerException(
            404, {"result": "View not found", "code": "ViewIdDoesNotExist"}
        )  # noqa: E501

    return True


def get_view_entity_id(view_id: str) -> str:
    """Function that returns the entity id related to the view

    Args:
        view_id (str): view uuid

    Raises:
        ControllerException: raise if the view is not related to an entity

    Returns:
        str: UUID of the view's entity
    """
    sql = SQLTable().select("views_master", ["entity_id"]).where([f"view_id='{view_id}'"]).query
    response = deserialize_rds_response(rds_execute_statement(sql))

    if not response:
        raise ControllerException(
            404, {"result": "View not found", "code": "ViewIdDoesNotExist"}
        )  # noqa: E501

    return response[0]["entity_id"]


def check_field_entity(entity_id: str = None, custom_field_id: str = None) -> bool:
    """Function to check if the field entity and view entity are the same
    TODO: accept FK entities fields

    Args:
        entity_id (str, optional): UUID of the view entity. Defaults to None.
        custom_field_id (str, optional): custom field UUID. Defaults to None.

    Raises:
        ControllerException: raise when the entity id is null
        ControllerException: raise if the entity id from the view is not the same as the
        field entity

    Returns:
        bool: True if the custom field entity and the view entity is the same
    """
    if custom_field_id is None:
        return False

    if not (entity_id):
        raise ControllerException(
            404, {"result": "View not found", "code": "ViewIdDoesNotExist"}
        )  # noqa: E501

    sql = (
        SQLTable()
        .select("custom_fields_master", ["COUNT(*)"])
        .where([f"custom_field_id='{custom_field_id}'", f"parent_object_id='{entity_id}'"])
        .query
    )
    count = deserialize_rds_response(rds_execute_statement(sql))[0]["count"]

    if not count:
        raise ControllerException(
            404,
            {
                "result": f"The field {custom_field_id} entity and view entity are not the same",
                "code": "FieldAndViewEntityDoesNotMatch",
            },
        )  # noqa: E501

    return True


def validate_json_content(obj: dict) -> bool:
    try:
        if not obj:
            return True

        if "cards" not in obj or not isinstance(obj["cards"], list):
            return False

        for card in obj["cards"]:
            if not _validate_card(card):
                return False

        return True
    except json.JSONDecodeError:
        raise ControllerException(
            404,
            {
                "result": "Invalid json content format",
                "code": "ViewsPut.InvalidFormat",
            },
        )


def _validate_card(card: dict) -> bool:
    required_fields = ["card_id", "card_type", "title", "properties", "buttons"]
    for field in required_fields:
        if field not in card:
            return False

    card_type = card["card_type"]
    if card_type not in ["form", "data-grid", "widget"]:
        return False

    if card_type == "form":
        if not _validate_form_card(card):
            return False
    elif card_type == "data-grid":
        if not _validate_data_grid_card(card):
            return False

    if "buttons" in card and "header" in card["buttons"]:
        header_buttons = card["buttons"]["header"]
        if header_buttons:
            for button in header_buttons:
                if "type" in button and button["type"] in ["save", "edit"]:
                    raise ControllerException(
                        404,
                        {
                            "result": f"Invalid button type in header: {button['type']}",
                            "code": "ViewsPut.InvalidButton",
                        },
                    )

    return True


def _validate_form_card(card: dict) -> bool:
    required_fields = ["sections"]
    for field in required_fields:
        if field not in card or not isinstance(card[field], list):
            return False

    for section in card["sections"]:
        if not _validate_form_section(section):
            return False

    return True


def _validate_data_grid_card(card: dict) -> bool:
    required_fields = ["sections"]
    for field in required_fields:
        if field not in card or not isinstance(card[field], list):
            return False

    for section in card["sections"]:
        if not _validate_data_grid_section(section):
            return False

    return True


def _validate_form_section(section: dict) -> bool:
    required_fields = ["fields"]
    for field in required_fields:
        if field not in section or not isinstance(section[field], list):
            return False

    for field in section["fields"]:
        if not _validate_field(field):
            return False

    return True


def _validate_data_grid_section(section: dict) -> bool:
    required_fields = ["columns", "column_visibility_model", "data_source", "datagrid_type"]
    for field in required_fields:
        if field not in section:
            return False

    return True


def _validate_field(field: dict) -> bool:
    required_fields = [
        "custom_field_id",
        "column_name",
        "name",
        "input",
        "data_type",
        "is_required",
    ]
    for field_name in required_fields:
        if field_name not in field:
            return False

    return True
